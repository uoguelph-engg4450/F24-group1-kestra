package io.kestra.core.tasks.flows;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.NextTaskRun;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.tasks.ResolvedTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.WorkerTask;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.validations.WorkingDirectoryTaskValidation;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.Collections;
import java.util.List;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run tasks sequentially in the same working directory",
    description = "Tasks are stateless by default. Kestra will launch each task within a temporary working directory on a Worker.\n" +
        "The `WorkingDirectory` task allows reusing the same file system's working directory across multiple tasks \n" +
        "so that multiple sequential tasks can use output files from previous tasks without having to use the `{{outputs.taskId.outputName}}` syntax." +
        "Note that the `WorkingDirectory` only works with runnable tasks because those tasks are executed directly on the Worker." +
        "This means that using flowable tasks such as the `Parallel` task within the `WorkingDirectory` task will not work." +
        "The `WorkingDirectory` task requires Kestra>=0.9.0."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = {
                "id: working-directory",
                "namespace: io.kestra.tests",
                "",
                "tasks:",
                "  - id: working-directory",
                "    type: io.kestra.core.tasks.flows.WorkingDirectory",
                "    tasks:",
                "      - id: first",
                "        type: io.kestra.core.tasks.scripts.Bash",
                "        commands:",
                "        - 'echo \"{{ taskrun.id }}\" > {{ workingDir }}/stay.txt'",
                "      - id: second",
                "        type: io.kestra.core.tasks.scripts.Bash",
                "        commands:",
                "        - |",
                "          echo '::{\"outputs\": {\"stay\":\"'$(cat {{ workingDir }}/stay.txt)'\"}}::'"
            }
        )
    }
)
@WorkingDirectoryTaskValidation
public class WorkingDirectory extends Sequential {

    @Override
    public List<NextTaskRun> resolveNexts(RunContext runContext, Execution execution, TaskRun parentTaskRun) throws IllegalVariableEvaluationException {
        List<ResolvedTask> childTasks = this.childTasks(runContext, parentTaskRun);

        if (execution.hasFailed(childTasks, parentTaskRun)) {
            return super.resolveNexts(runContext, execution, parentTaskRun);
        }

        // resolve to no next tasks as the worker will execute all tasks
        return Collections.emptyList();
    }

    public WorkerTask workerTask(TaskRun parent, Task task, RunContext runContext) {
        return WorkerTask.builder()
            .task(task)
            .taskRun(TaskRun.builder()
                .id(IdUtils.create())
                .executionId(parent.getExecutionId())
                .namespace(parent.getNamespace())
                .flowId(parent.getFlowId())
                .taskId(task.getId())
                .parentTaskRunId(parent.getId())
                .state(new State())
                .build()
            )
            .runContext(runContext)
            .build();
    }
}
