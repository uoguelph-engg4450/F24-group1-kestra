package org.kestra.core.models.tasks;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.micronaut.core.annotation.Introspected;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.models.executions.TaskRun;
import org.kestra.core.models.tasks.retrys.AbstractRetry;
import org.kestra.core.runners.RunContext;

import java.util.Optional;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import static org.kestra.core.utils.Rethrow.throwFunction;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "type", visible = true, include = JsonTypeInfo.As.EXISTING_PROPERTY)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@SuperBuilder
@Getter
@NoArgsConstructor
@Introspected
abstract public class Task {
    @NotNull
    @NotBlank
    @Pattern(regexp="[a-zA-Z0-9_-]+")
    protected String id;

    @NotNull
    @NotBlank
    @Pattern(regexp="\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*(\\.\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*)*")
    protected String type;

    private String description;

    @Valid
    protected AbstractRetry retry;

    @Min(0)
    protected Integer timeout;

    public Optional<Task> findById(String id) {
        if (this.getId().equals(id)) {
            return Optional.of(this);
        }

        if (this.isFlowable()) {
            Optional<Task> childs = ((FlowableTask<?>) this).allChildTasks()
                .stream()
                .map(t -> t.findById(id))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();

            if (childs.isPresent()) {
                return childs;
            }
        }

        return Optional.empty();
    }

    public Optional<Task> findById(String id, RunContext runContext, TaskRun taskRun) throws IllegalVariableEvaluationException {
        if (this.getId().equals(id)) {
            return Optional.of(this);
        }

        if (this.isFlowable()) {
            Optional<Task> childs = ((FlowableTask<?>) this).childTasks(runContext, taskRun)
                .stream()
                .map(throwFunction(resolvedTask -> resolvedTask.getTask().findById(id, runContext, taskRun)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();

            if (childs.isPresent()) {
                return childs;
            }
        }

        if (this.isFlowable() && ((FlowableTask<?>) this).getErrors() != null) {
            Optional<Task> errorChilds = ((FlowableTask<?>) this).getErrors()
                .stream()
                .map(throwFunction(task -> task.findById(id, runContext, taskRun)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();

            if (errorChilds.isPresent()) {
                return errorChilds;
            }
        }

        return Optional.empty();
    }

    @JsonIgnore
    public boolean isFlowable() {
        return this instanceof FlowableTask;
    }
}
