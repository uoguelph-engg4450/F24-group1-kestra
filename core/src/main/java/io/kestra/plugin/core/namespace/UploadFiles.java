package io.kestra.plugin.core.namespace;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.Namespace;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.kestra.core.utils.PathUtil.checkLeadingSlash;

@SuperBuilder(toBuilder = true)
@Getter
@NoArgsConstructor
@Schema(
    title = "Upload one or multiple files to a specific namespace.",
    description = "Use a regex glob pattern or a file path to upload files as Namespace Files. When using a map with the desired file name as key and file path as value, you can also rename or relocate files."
)
@Plugin(
    examples = {
        @Example(
            title = "Upload files generated by a previous task using the `filesMap` property.",
            full = true,
            code = """
id: upload_files_from_git
namespace: company.team

tasks:
  - id: download
    type: io.kestra.plugin.core.http.Download
    uri: https://github.com/kestra-io/scripts/archive/refs/heads/main.zip
 \s
  - id: unzip
    type: io.kestra.plugin.compress.ArchiveDecompress
    from: "{{ outputs.download.uri }}"
    algorithm: ZIP

  - id: upload
    type: io.kestra.plugin.core.namespace.UploadFiles
    filesMap: "{{ outputs.unzip.files }}"
    namespace: "{{ flow.namespace }}"
    """
        ),
        @Example(
            title = "Upload a folder using a glob pattern. Note that the Regex syntax requires a `glob` pattern inspired by [Apache Ant patterns](https://ant.apache.org/manual/dirtasks.html#patterns). Make sure that your pattern starts with `glob:`, followed by the pattern. For example, use `glob:**/dbt/**` to upload the entire `dbt` folder (with all files and subdirectories) regardless of that folder's location in the directory structure.",
            full = true,
            code = """
id: upload_dbt_project
namespace: dwh
tasks:
  - id: wdir
    type: io.kestra.plugin.core.flow.WorkingDirectory
    tasks:
      - id: git_clone
        type: io.kestra.plugin.git.Clone
        url: https://github.com/kestra-io/dbt-example
        branch: master
      - id: upload
        type: io.kestra.plugin.core.namespace.UploadFiles
        files:
          - "glob:**/dbt/**"
        namespace: "{{ flow.namespace }}"
        """
        ),
        @Example(
            title = "Upload a specific file and rename it.",
            full = true,
            code = """
id: upload_a_file
namespace: dwh

tasks:
  - id: download
    type: io.kestra.plugin.core.http.Download
    uri: https://github.com/kestra-io/scripts/archive/refs/heads/main.zip
 \s
  - id: unzip
    type: io.kestra.plugin.compress.ArchiveDecompress
    from: "{{ outputs.download.uri }}"
    algorithm: ZIP

  - id: upload
    type: io.kestra.plugin.core.namespace.UploadFiles
    filesMap:
      LICENCE: "{{ outputs.unzip.files['scripts-main/LICENSE'] }}"
    namespace: "{{ flow.namespace }}"
    """
        )
    }
)
public class UploadFiles extends Task implements RunnableTask<UploadFiles.Output> {
    @NotNull
    @Schema(
        title = "The namespace to which the files will be uploaded."
    )
    @PluginProperty(dynamic = true)
    private String namespace;

    @Schema(
        title = "A list of Regex that match files in the current directory.",
        description = "This should be a list of Regex matching the [Apache Ant patterns](https://ant.apache.org/manual/dirtasks.html#patterns)." +
            "It's primarily intended to be used with the `WorkingDirectory` task"
    )
    @PluginProperty(dynamic = true)
    private List<String> files;

    @Schema(
        title = "A map of key-value pairs where the key is the filename and the value is the URI of the file to upload.",
        description = "This should be a map of URI, with the key being the filename that will be upload, and the key the URI." +
            "This one is intended to be used with output files of other tasks. Many Kestra tasks, incl. all Downloads tasks, " +
            "output a map of files so that you can directly pass the output property to this task e.g. " +
            "[outputFiles in the S3 Downloads task](https://kestra.io/plugins/plugin-aws/tasks/s3/io.kestra.plugin.aws.s3.downloads#outputfiles) " +
            "or the [files in the Archive Decompress task](https://kestra.io/plugins/plugin-compress/tasks/io.kestra.plugin.compress.archivedecompress#files).",
        anyOf = {Map.class, String.class}
    )
    @PluginProperty(dynamic = true)
    private Object filesMap;

    @Schema(
        title = "The destination folder.",
        description = "Required when providing a list of files."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private String destination = "/";

    @Builder.Default

    @Schema(
        title = "Which action to take when uploading a file that already exists.",
        description = "Can be one of the following options: OVERWRITE, ERROR or SKIP. Default is OVERWRITE."
    )
    private Namespace.Conflicts conflict = Namespace.Conflicts.OVERWRITE;

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public UploadFiles.Output run(RunContext runContext) throws Exception {
        RunContext.FlowInfo flowInfo = runContext.flowInfo();
        String renderedNamespace = namespace != null ? runContext.render(namespace) : flowInfo.namespace();
        String renderedDestination = checkLeadingSlash(runContext.render(destination));

        final Namespace storageNamespace = runContext.storage().namespace(renderedNamespace);

        if (files == null && filesMap == null) {
            throw new IllegalArgumentException("files or filesMap is required");
        }

        if (files != null) {
            this.uploadFiles(runContext, files, storageNamespace, renderedDestination);
        }

        if (filesMap != null) {
            Map<String, Object> readFilesMap = new HashMap<>();
            if (filesMap instanceof String) {
                String renderedFilesMap = runContext.render((String) filesMap);
                readFilesMap = JacksonMapper.ofJson().readValue(renderedFilesMap, Map.class);
            } else {
                readFilesMap = (Map<String, Object>) filesMap;
            }
            this.uploadFilesMap(runContext, readFilesMap, storageNamespace, renderedDestination);
        }

        return Output.builder().build();
    }

    private void uploadFiles(RunContext runContext, List<String> files, Namespace storageNamespace, String destination) throws IllegalVariableEvaluationException, IOException, URISyntaxException {
        files = runContext.render(files);

        for (Path path : runContext.workingDir().findAllFilesMatching(files)) {
            File file = path.toFile();
            Path resolve = Paths.get("/").resolve(runContext.workingDir().path().relativize(file.toPath()));

            Path targetFilePath = Path.of(destination, resolve.toString());
            storageNamespace.putFile(targetFilePath, new FileInputStream(file), conflict);
        }
    }

    private void uploadFilesMap(RunContext runContext, Map<String, Object> filesMap, Namespace storageNamespace, String destination) throws IOException, URISyntaxException, IllegalVariableEvaluationException {
        Map<String, Object> renderedMap = runContext.render(filesMap);
        for (Map.Entry<String, Object> entry : renderedMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (key instanceof String targetFilePath && value instanceof String stringSourceFileURI) {
                URI sourceFileURI = URI.create(stringSourceFileURI);
                if (runContext.storage().isFileExist(sourceFileURI)) {
                    storageNamespace.putFile(Path.of(destination + targetFilePath), runContext.storage().getFile(sourceFileURI), conflict);
                }
            } else {
                throw new IllegalArgumentException("filesMap must be a Map<String, String>");
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private final Map<String, URI> files;
    }
}
