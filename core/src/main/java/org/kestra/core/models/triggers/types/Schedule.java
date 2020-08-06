package org.kestra.core.models.triggers.types;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.executions.Execution;
import org.kestra.core.models.flows.State;
import org.kestra.core.models.triggers.AbstractTrigger;
import org.kestra.core.models.triggers.PollingTriggerInterface;
import org.kestra.core.models.triggers.TriggerContext;
import org.kestra.core.schedulers.validations.CronExpression;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class Schedule extends AbstractTrigger implements PollingTriggerInterface {
    private static final CronParser CRON_PARSER = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));

    @NotNull
    @CronExpression
    private String cron;

    private ScheduleBackfill backfill;

    private Duration interval;

    public ZonedDateTime nextDate(Optional<? extends TriggerContext> last) {
        if (last.isPresent()) {
            return computeNextDate(last.get().getDate()).orElse(null);
        } else {
            if (backfill != null && backfill.getStart() != null) {
                return backfill.getStart();
            }

            return computeNextDate(ZonedDateTime.now(ZoneId.systemDefault())).orElse(null);
        }
    }

    public Optional<Execution> evaluate(TriggerContext context) {
        Cron parse = CRON_PARSER.parse(this.cron);

        ExecutionTime executionTime = ExecutionTime.forCron(parse);
        Optional<ZonedDateTime> next = executionTime.nextExecution(context.getDate().minus(Duration.ofSeconds(1)));

        if (next.isEmpty()) {
            return Optional.empty();
        }

        // we try at the exact time / standard behaviour
        boolean isReady = next.get().toEpochSecond() == context.getDate().toEpochSecond();

        // in case on cron expression changed, the next date will never match so we allow past operation to start
        boolean isLate = next.get().toEpochSecond() < ZonedDateTime.now(ZoneId.systemDefault()).minus(Duration.ofMinutes(1)).toEpochSecond();

        if (!isReady && !isLate) {
            return Optional.empty();
        }

        // we are in the future don't allow
        if (next.get().toEpochSecond() > ZonedDateTime.now(ZoneId.systemDefault()).plus(Duration.ofSeconds(1)).toEpochSecond()) {
            return Optional.empty();
        }

        ImmutableMap.Builder<Object, Object> vars = ImmutableMap.builder()
            .put("date", next.get());

        computeNextDate(next.get())
            .ifPresent(zonedDateTime -> vars.put("next", zonedDateTime));

        executionTime.lastExecution(context.getDate())
            .ifPresent(zonedDateTime -> vars.put("previous", zonedDateTime));

        Execution execution = Execution.builder()
            .id(FriendlyId.createFriendlyId())
            .namespace(context.getNamespace())
            .flowId(context.getFlowId())
            .flowRevision(context.getFlowRevision())
            .state(new State())
            .variables(ImmutableMap.of(
                "schedule", vars.build()
            ))
            .build();

        return Optional.of(execution);
    }

    private Optional<ZonedDateTime> computeNextDate(ZonedDateTime date) {
        Cron parse = CRON_PARSER.parse(this.cron);
        ExecutionTime executionTime = ExecutionTime.forCron(parse);

        return executionTime.nextExecution(date).map(zonedDateTime -> zonedDateTime.truncatedTo(ChronoUnit.SECONDS));
    }
}
