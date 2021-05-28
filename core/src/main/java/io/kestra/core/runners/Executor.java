package io.kestra.core.runners;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.flows.Flow;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
@AllArgsConstructor
public class Executor {
    private Execution execution;
    private Exception exception;
    private final List<String> from = new ArrayList<>();
    private Long offset;
    private boolean executionUpdated = false;
    private Flow flow;
    private final List<TaskRun> nexts = new ArrayList<>();
    private final List<WorkerTask> workerTasks = new ArrayList<>();
    private final List<WorkerTaskResult> workerTaskResults = new ArrayList<>();
    private WorkerTaskResult joined;

    public Executor(Execution execution, Long offset) {
        this.execution = execution;
        this.offset = offset;
    }

    public Executor(WorkerTaskResult workerTaskResult) {
        this.joined = workerTaskResult;
    }

    public Executor withFlow(Flow flow) {
        this.flow = flow;

        return this;
    }

    public Executor withExecution(Execution execution, String from) {
        this.execution = execution;
        this.from.add(from);
        this.executionUpdated = true;

        return this;
    }

    public Executor withException(Exception exception, String from) {
        this.exception = exception;
        this.from.add(from);
        this.executionUpdated = true;

        return this;
    }

    public Executor withTaskRun(List<TaskRun> taskRuns, String from) {
        this.nexts.addAll(taskRuns);
        this.from.add(from);

        return this;
    }

    public Executor withWorkerTasks(List<WorkerTask> workerTasks, String from) {
        this.workerTasks.addAll(workerTasks);
        this.from.add(from);

        return this;
    }

    public Executor withWorkerTaskResults(List<WorkerTaskResult> workerTaskResults, String from) {
        this.workerTaskResults.addAll(workerTaskResults);
        this.from.add(from);

        return this;
    }

    public Executor serialize() {
        return new Executor(
            execution,
            this.offset
        );
    }
}
