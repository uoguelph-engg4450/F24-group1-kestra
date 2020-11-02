package org.kestra.repository.elasticsearch;

import com.devskiller.friendly_id.FriendlyId;
import io.micronaut.data.model.Pageable;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kestra.core.models.executions.Execution;
import org.kestra.core.models.executions.TaskRun;
import org.kestra.core.models.executions.statistics.DailyExecutionStatistics;
import org.kestra.core.models.flows.State;
import org.kestra.core.models.tasks.ResolvedTask;
import org.kestra.core.repositories.ArrayListTotal;
import org.kestra.core.tasks.debugs.Return;

import javax.inject.Inject;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

@MicronautTest
class ElasticSearchExecutionRepositoryTest {
    public static final String NAMESPACE = "org.kestra.unittest";
    public static final String FLOW = "full";

    @Inject
    ElasticSearchExecutionRepository executionRepository;

    @Inject
    ElasticSearchRepositoryTestUtils utils;

    static Execution.ExecutionBuilder builder(State.Type state, String flowId) {
        State finalState = new State();

        finalState = spy(finalState
            .withState(state != null ? state : State.Type.SUCCESS));

        Random rand = new Random();
        doReturn(Duration.ofSeconds(rand.nextInt(150)))
            .when(finalState)
            .getDuration();

        Execution.ExecutionBuilder execution = Execution.builder()
            .id(FriendlyId.createFriendlyId())
            .namespace(NAMESPACE)
            .flowId(flowId == null ? FLOW : flowId)
            .flowRevision(1)
            .state(finalState);


        List<TaskRun> taskRuns = Arrays.asList(
            TaskRun.of(execution.build(), ResolvedTask.of(
                Return.builder().id("first").type(Return.class.getName()).format("test").build())
            )
                .withState(State.Type.SUCCESS),
            TaskRun.of(execution.build(), ResolvedTask.of(
                Return.builder().id("second").type(Return.class.getName()).format("test").build())
            )
                .withState(state),
            TaskRun.of(execution.build(), ResolvedTask.of(
                Return.builder().id("third").type(Return.class.getName()).format("test").build())).withState(state)
        );

        if (flowId == null) {
            return execution.taskRunList(List.of(taskRuns.get(0), taskRuns.get(1), taskRuns.get(2)));
        }

        return execution.taskRunList(List.of(taskRuns.get(0), taskRuns.get(1)));
    }

    void inject() {
        for (int i = 0; i < 28; i++) {
            executionRepository.save(builder(
                i < 5 ? State.Type.RUNNING : (i < 8 ? State.Type.FAILED : State.Type.SUCCESS),
                i < 15 ? null : "second"
            ).build());
        }
    }

    @Test
    void find() {
        inject();

        ArrayListTotal<Execution> executions = executionRepository.find("*", Pageable.from(1, 10), null);
        assertThat(executions.getTotal(), is(28L));
        assertThat(executions.size(), is(10));
    }

    @Test
    void findTaskRun() {
        inject();

        ArrayListTotal<TaskRun> executions = executionRepository.findTaskRun("*", Pageable.from(1, 10), null);
        assertThat(executions.getTotal(), is(71L));
        assertThat(executions.size(), is(10));
    }


    @Test
    void findById() {
        executionRepository.save(ExecutionFixture.EXECUTION_1);

        Optional<Execution> full = executionRepository.findById(ExecutionFixture.EXECUTION_1.getId());
        assertThat(full.isPresent(), is(true));

        full.ifPresent(current -> {
            assertThat(full.get().getId(), is(ExecutionFixture.EXECUTION_1.getId()));
        });
    }

    @Test
    void mappingConflict() {
        executionRepository.save(ExecutionFixture.EXECUTION_2);
        executionRepository.save(ExecutionFixture.EXECUTION_1);

        ArrayListTotal<Execution> page1 = executionRepository.findByFlowId(NAMESPACE, FLOW, Pageable.from(1, 10));

        assertThat(page1.size(), is(2));
    }

    @Test
    void dailyGroupByFlowStatistics() {
        for (int i = 0; i < 28; i++) {
            executionRepository.save(builder(
                i < 5 ? State.Type.RUNNING : (i < 8 ? State.Type.FAILED : State.Type.SUCCESS),
                i < 15 ? null : "second"
            ).build());
        }

        Map<String, Map<String, List<DailyExecutionStatistics>>> result = executionRepository.dailyGroupByFlowStatistics(
            "*",
            LocalDate.now().minusDays(10),
            LocalDate.now()
        );

        assertThat(result.size(), is(1));
        assertThat(result.get("org.kestra.unittest").size(), is(2));

        DailyExecutionStatistics full = result.get("org.kestra.unittest").get(FLOW).get(10);
        DailyExecutionStatistics second = result.get("org.kestra.unittest").get("second").get(10);

        assertThat(full.getDuration().getAvg().getSeconds(), greaterThan(0L));
        assertThat(full.getExecutionCounts().size(), is(7));
        assertThat(full.getExecutionCounts().get(State.Type.FAILED), is(3L));
        assertThat(full.getExecutionCounts().get(State.Type.RUNNING), is(5L));
        assertThat(full.getExecutionCounts().get(State.Type.SUCCESS), is(7L));
        assertThat(full.getExecutionCounts().get(State.Type.CREATED), is(0L));

        assertThat(second.getDuration().getAvg().getSeconds(), greaterThan(0L));
        assertThat(second.getExecutionCounts().size(), is(7));
        assertThat(second.getExecutionCounts().get(State.Type.SUCCESS), is(13L));
        assertThat(second.getExecutionCounts().get(State.Type.CREATED), is(0L));
    }

    @Test
    void dailyStatistics() {
        for (int i = 0; i < 28; i++) {
            executionRepository.save(builder(
                i < 5 ? State.Type.RUNNING : (i < 8 ? State.Type.FAILED : State.Type.SUCCESS),
                i < 15 ? null : "second"
            ).build());
        }

        List<DailyExecutionStatistics> result = executionRepository.dailyStatistics(
            "*",
            LocalDate.now().minusDays(10),
            LocalDate.now()
        );

        assertThat(result.size(), is(11));
        assertThat(result.get(10).getExecutionCounts().size(), is(7));
        assertThat(result.get(10).getDuration().getAvg().getSeconds(), greaterThan(0L));

        assertThat(result.get(10).getExecutionCounts().get(State.Type.FAILED), is(3L));
        assertThat(result.get(10).getExecutionCounts().get(State.Type.RUNNING), is(5L));
        assertThat(result.get(10).getExecutionCounts().get(State.Type.SUCCESS), is(20L));
    }

    @AfterEach
    protected void tearDown() throws IOException {
        utils.tearDown();
        executionRepository.initMapping();
    }
}
