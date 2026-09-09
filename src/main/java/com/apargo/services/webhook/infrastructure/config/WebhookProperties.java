package com.apargo.services.webhook.infrastructure.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

/**
 * Every tunable this service has, in one place, bound from {@code application.yml} which in turn
 * reads only environment variables. Nothing else in the codebase reads configuration directly.
 *
 * <p>This record says what configuration exists and what shape it has; {@link WebhookDefaults} says
 * what each value is when nobody sets one, and why.
 */
@Validated
@ConfigurationProperties(prefix = "webhook")
public record WebhookProperties(
        @Valid @NotNull Meta meta,
        @Valid @NotNull Scheduling scheduling,
        @Valid @NotNull Ingest ingest,
        @Valid @NotNull Publisher publisher,
        @Valid @NotNull Relay relay,
        @Valid @NotNull Topics topics,
        @Valid @NotNull Retention retention,
        @Valid @NotNull Internal internal) {

    /**
     * Meta credentials. Neither secret has a default: a secret with a fallback value is a secret in
     * git history. They are validated at startup by {@link WebhookPropertiesValidator}.
     *
     * @param verifySignature set false ONLY in local development, never in a deployed environment
     */
    public record Meta(
            @DefaultValue(WebhookDefaults.META_SECRET) String verifyToken,
            @DefaultValue(WebhookDefaults.META_SECRET) String appSecret,
            @DefaultValue(WebhookDefaults.META_SIGNATURE_HEADER) @NotBlank String signatureHeader,
            @DefaultValue(WebhookDefaults.META_VERIFY_SIGNATURE) boolean verifySignature) {
    }

    /**
     * The scheduler pool, shared by the relay drain, the lease reclaim sweep and the metrics
     * refresh.
     *
     * <p>This is the single source of truth for the pool size: {@link SchedulingConfig} builds the
     * {@code taskScheduler} bean from it, which makes Boot's own
     * {@code spring.task.scheduling.pool.size} inert. Previously the two could drift, and the
     * symptom of drifting low is a stalled gauge rather than an error.
     *
     * @param poolSize must exceed the number of concurrent schedules
     */
    public record Scheduling(
            @DefaultValue(WebhookDefaults.SCHEDULING_POOL_SIZE) @Min(2) int poolSize) {
    }

    /**
     * @param maxPayloadSize Meta's documented ceiling; a larger body is stored truncated, not dropped
     * @param dedupeTtl      how long a body hash is remembered
     * @param batch          cross-request insert batching, off by default
     */
    public record Ingest(
            @DefaultValue(WebhookDefaults.INGEST_MAX_PAYLOAD_SIZE) @NotNull DataSize maxPayloadSize,
            @DefaultValue(WebhookDefaults.INGEST_DEDUPE_TTL) @NotNull Duration dedupeTtl,
            @Valid @DefaultValue Batch batch) {

        /**
         * Accumulates webhooks from separate HTTP requests into one bulk insert, closing on
         * whichever trigger fires first: records, bytes or elapsed time.
         *
         * <p>Off by default, and that is a position rather than caution. {@code insertAll} is
         * already one bulk write per request, so batching across requests trades latency on the one
         * path where Meta is waiting against a saving on the cheapest round trip in the pipeline.
         * Turn it on against a profile, not a hunch.
         *
         * @param maxRecords fires first under load
         * @param maxBytes   estimated serialised BSON, not raw request bytes
         * @param maxWait    added directly to response time; governs light traffic only
         */
        public record Batch(
                @DefaultValue(WebhookDefaults.INGEST_BATCH_ENABLED) boolean enabled,
                @DefaultValue(WebhookDefaults.INGEST_BATCH_MAX_RECORDS) @Min(1) int maxRecords,
                @DefaultValue(WebhookDefaults.INGEST_BATCH_MAX_BYTES) @NotNull DataSize maxBytes,
                @DefaultValue(WebhookDefaults.INGEST_BATCH_MAX_WAIT) @NotNull Duration maxWait) {
        }
    }

    /**
     * The fast path: the in-memory batch handed straight to Kafka once Mongo has acknowledged it,
     * rather than written and immediately read back.
     *
     * @param fastPathEnabled  false routes every event through the recovery worker
     * @param maxInFlight      backpressure bound; past it, records simply stay PENDING
     * @param ackFlushSize     ids per bulk PUBLISHED mark
     * @param ackFlushInterval must be at most {@code relay.grace / 3} — see
     *                         {@link #isAckFlushWithinGrace()}
     */
    public record Publisher(
            @DefaultValue(WebhookDefaults.PUBLISHER_FAST_PATH_ENABLED) boolean fastPathEnabled,
            @DefaultValue(WebhookDefaults.PUBLISHER_MAX_IN_FLIGHT) @Min(1) int maxInFlight,
            @DefaultValue(WebhookDefaults.PUBLISHER_ACK_FLUSH_SIZE) @Min(1) int ackFlushSize,
            @DefaultValue(WebhookDefaults.PUBLISHER_ACK_FLUSH_INTERVAL) @NotNull Duration ackFlushInterval) {
    }

    /**
     * The recovery worker. Once the fast path is on, this stops being the pipeline and becomes what
     * it should always have been: the thing that catches what the fast path dropped.
     *
     * <p>Kafka outages are usually minutes and no user is waiting, so the schedule is deliberately
     * patient.
     *
     * @param grace how long a freshly inserted record is invisible to the recovery worker
     */
    public record Relay(
            @DefaultValue(WebhookDefaults.RELAY_ENABLED) boolean enabled,
            @DefaultValue(WebhookDefaults.RELAY_POLL_INTERVAL) @NotNull Duration pollInterval,
            @DefaultValue(WebhookDefaults.RELAY_BATCH_SIZE) @Min(1) int batchSize,
            @DefaultValue(WebhookDefaults.RELAY_GRACE) @NotNull Duration grace,
            @DefaultValue(WebhookDefaults.RELAY_LEASE) @NotNull Duration lease,
            @DefaultValue(WebhookDefaults.RELAY_LEASE_RECLAIM_INTERVAL) @NotNull Duration leaseReclaimInterval,
            @DefaultValue(WebhookDefaults.RELAY_MAX_ATTEMPTS) @Min(1) int maxAttempts,
            @DefaultValue(WebhookDefaults.RELAY_BACKOFF_BASE) @NotNull Duration backoffBase,
            @DefaultValue(WebhookDefaults.RELAY_BACKOFF_MAX) @NotNull Duration backoffMax,
            @DefaultValue(WebhookDefaults.RELAY_METRICS_INTERVAL) @NotNull Duration metricsInterval) {

        /** Backoff that shrinks with each attempt is a configuration error, not a strategy. */
        public boolean isBackoffRangeOrdered() {
            return backoffMax.compareTo(backoffBase) >= 0;
        }
    }

    /** Topic names and their partition counts. One topic per lane; consumers filter. */
    public record Topics(
            @DefaultValue(WebhookDefaults.TOPIC_INBOUND) @NotBlank String inbound,
            @DefaultValue(WebhookDefaults.TOPIC_STATUS) @NotBlank String status,
            @DefaultValue(WebhookDefaults.TOPIC_TEMPLATE) @NotBlank String template,
            @DefaultValue(WebhookDefaults.TOPIC_ACCOUNT) @NotBlank String account,
            @DefaultValue(WebhookDefaults.TOPIC_USER_PREFERENCE) @NotBlank String userPreference,
            @DefaultValue(WebhookDefaults.TOPIC_UNROUTED) @NotBlank String unrouted,
            @DefaultValue(WebhookDefaults.TOPIC_AUTO_CREATE) boolean autoCreate,
            @DefaultValue(WebhookDefaults.TOPIC_REPLICAS) @Min(1) int replicas,
            @Valid @NotNull Partitions partitions) {

        public record Partitions(
                @DefaultValue(WebhookDefaults.PARTITIONS_INBOUND) @Min(1) int inbound,
                @DefaultValue(WebhookDefaults.PARTITIONS_STATUS) @Min(1) int status,
                @DefaultValue(WebhookDefaults.PARTITIONS_TEMPLATE) @Min(1) int template,
                @DefaultValue(WebhookDefaults.PARTITIONS_ACCOUNT) @Min(1) int account,
                @DefaultValue(WebhookDefaults.PARTITIONS_USER_PREFERENCE) @Min(1) int userPreference,
                @DefaultValue(WebhookDefaults.PARTITIONS_UNROUTED) @Min(1) int unrouted) {
        }
    }

    /** @param eventTtl drives the TTL index on {@code webhook_events.receivedAt} */
    public record Retention(
            @DefaultValue(WebhookDefaults.RETENTION_EVENT_TTL) @NotNull Duration eventTtl) {
    }

    /** Internal support plane, guarded exactly as the other services on the platform. */
    public record Internal(
            @DefaultValue(WebhookDefaults.INTERNAL_API_KEY) String apiKey,
            @DefaultValue(WebhookDefaults.INTERNAL_HEADER_NAME) @NotBlank String headerName) {
    }

    // -----------------------------------------------------------------------
    // Cross-section invariants, checked at startup rather than discovered under load
    // -----------------------------------------------------------------------

    /**
     * The one relationship that silently duplicates the entire stream when it is wrong.
     *
     * <p>A record is PENDING from the moment it is inserted until the ack flusher marks it
     * PUBLISHED, while the in-memory batch is already in flight to Kafka. The grace window is what
     * hides it from the recovery worker for that interval. If the flush interval is not comfortably
     * inside the grace, a slow write or a missed tick leaves records visible to the recovery worker
     * that the fast path has already delivered — and every one of them is published twice.
     *
     * <p>Nothing fails when this is violated. Throughput looks fine and consumers, which dedupe on
     * wamid, absorb it. It surfaces as unexplained duplicate volume, which is why it is asserted at
     * startup instead.
     */
    public boolean isAckFlushWithinGrace() {
        return publisher.ackFlushInterval()
                .multipliedBy(WebhookDefaults.ACK_FLUSH_INTERVALS_PER_GRACE)
                .compareTo(relay.grace()) <= 0;
    }
}
