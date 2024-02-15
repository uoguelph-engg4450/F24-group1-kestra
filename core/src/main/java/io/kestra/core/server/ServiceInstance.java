package io.kestra.core.server;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.kestra.core.server.Service.ServiceState;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Runtime information about a Kestra's service (e.g., WORKER, EXECUTOR, etc.).
 *
 * @param id        The service unique identifier.
 * @param type      The service type.
 * @param state     The state of the service.
 * @param server    The server running this service.
 * @param createdAt Instant when this service was created.
 * @param updatedAt Instant when this service was updated.
 * @param events    The last of events attached to this service - used to provide some contextual information about a state changed.
 * @param config    The server configuration and liveness.
 * @param props     The server additional properties - an opaque map of key/value pairs.
 * @param seqId     A monolithic sequence id which is incremented each time the service instance is updated.
 *                  Used to detect non-transactional update of the instance.
 */
@JsonInclude(JsonInclude.Include.ALWAYS)
public record ServiceInstance(
    String id,
    Service.ServiceType type,
    ServiceState state,
    ServerInstance server,
    Instant createdAt,
    Instant updatedAt,
    List<TimestampedEvent> events,
    ServerConfig config,
    Map<String, Object> props,
    long seqId
) {

    public ServiceInstance(
        String id,
        Service.ServiceType type,
        ServiceState state,
        ServerInstance server,
        Instant createdAt,
        Instant updatedAt,
        List<TimestampedEvent> events,
        ServerConfig config,
        Map<String, Object> props
    ) {
        this(id, type, state, server, createdAt, updatedAt, events, config, props, 0L);
    }

    /**
     * Checks service type.
     *
     * @param type the type to check.
     * @return {@code true} if this instance is of the given type.
     */
    public boolean is(final Service.ServiceType type) {
        return this.type.equals(type);
    }

    /**
     * Check service state.
     *
     * @param state the state to check.
     * @return {@code true} if this instance is in the given state.
     */
    public boolean is(final ServiceState state) {
        return this.state.equals(state);
    }

    /**
     * Updates this service instance with the given state and instant.
     *
     * @param newState  The new state.
     * @param updatedAt The update instant
     * @return a new {@link ServiceInstance}.
     */
    public ServiceInstance updateState(final ServiceState newState,
                                       final Instant updatedAt) {
        return updateState(newState, updatedAt, null);
    }

    /**
     * Updates this service instance with the given state and instant.
     *
     * @param newState  The new state.
     * @param updatedAt The update instant
     * @param reason    The human-readable reason of the update.
     * @return a new {@link ServiceInstance}.
     */
    public ServiceInstance updateState(final ServiceState newState,
                                       final Instant updatedAt,
                                       final String reason) {

        List<TimestampedEvent> events = this.events;
        if (reason != null) {
            events = new ArrayList<>(events);
            events.add(new TimestampedEvent(updatedAt, reason));
        }

        long nextSeqId = seqId + 1;
        return new ServiceInstance(
            id,
            type,
            newState,
            server,
            createdAt,
            updatedAt,
            events,
            config,
            props,
            nextSeqId
        );
    }

    /**
     * Checks whether the session timeout elapsed for this service.
     *
     * @param now The instant.
     * @return {@code true} if the session for this service has timeout, otherwise {@code false}.
     */
    public boolean isSessionTimeoutElapsed(final Instant now) {
        Duration timeout = this.config.liveness().timeout();
        return this.state.isRunning() && updatedAt().plus(timeout).isBefore(now);
    }

    /**
     * Checks whether the termination grace period elapsed for this service.
     *
     * @param now The instant.
     * @return {@code true} if the termination grace period elapsed, otherwise {@code false}.
     */
    public boolean isTerminationGracePeriodElapsed(final Instant now) {
        Duration terminationGracePeriod = this.config.terminationGracePeriod();
        return this.updatedAt().plus(terminationGracePeriod).isBefore(now);
    }

    /**
     * A timestamped event value.
     *
     * @param ts    The instant of this event.
     * @param value The value of this event.
     */
    public record TimestampedEvent(Instant ts, String value) {
    }
}
