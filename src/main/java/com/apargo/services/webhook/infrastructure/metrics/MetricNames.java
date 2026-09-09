package com.apargo.services.webhook.infrastructure.metrics;

/**
 * Every meter name, tag key and tag value this service publishes.
 *
 * <p>Meter names are an external contract in the same way topic names are: they are hard-coded into
 * dashboards and alert rules that live in another repository, so a rename here silently blinds an
 * alert rather than breaking a build. Keeping them in one file makes the blast radius of a rename
 * visible before it ships.
 *
 * <p>Tag values are enumerated too, because a tag whose value comes from an unbounded source — an
 * exception message, a topic name, a phone number — creates one time series per distinct value and
 * takes the metrics backend down with it.
 */
public final class MetricNames {

    // --- Ingest ------------------------------------------------------------
    public static final String INGEST_RECEIVED = "webhook.ingest.received";
    public static final String INGEST_DUPLICATE = "webhook.ingest.duplicate";
    public static final String INGEST_REJECTED = "webhook.ingest.rejected";
    public static final String INGEST_LATENCY = "webhook.ingest.latency";

    // --- Relay and publishing ----------------------------------------------
    public static final String RELAY_PUBLISHED = "webhook.relay.published";
    public static final String EVENTS_PENDING = "webhook.events.pending";
    public static final String EVENTS_FAILED = "webhook.events.failed";

    /** The gauge worth alerting on: the age of the oldest undelivered event, not queue depth. */
    public static final String RELAY_LAG = "webhook.relay.lag";

    // --- Fast path ---------------------------------------------------------
    public static final String FASTPATH_HANDED_OFF = "webhook.fastpath.handed.off";

    /** Increments when the in-flight bound is hit. Any non-zero value means Kafka is behind. */
    public static final String FASTPATH_REJECTED = "webhook.fastpath.rejected";

    public static final String FASTPATH_FAILED = "webhook.fastpath.failed";
    public static final String FASTPATH_IN_FLIGHT = "webhook.fastpath.in.flight";

    // --- Acknowledgement flush ---------------------------------------------
    public static final String ACK_FLUSHED = "webhook.ack.flushed";
    public static final String ACK_QUEUE_DEPTH = "webhook.ack.queue.depth";

    // --- Tag keys ----------------------------------------------------------
    public static final String TAG_LANE = "lane";
    public static final String TAG_REASON = "reason";

    // --- Tag values. Low cardinality, and only these. -----------------------
    public static final String REASON_SIGNATURE = "signature";
    public static final String REASON_UNPARSEABLE = "unparseable";
    public static final String REASON_OVERSIZED = "oversized";

    // --- Units -------------------------------------------------------------
    public static final String UNIT_SECONDS = "seconds";
    public static final String UNIT_EVENTS = "events";

    private MetricNames() {
    }
}
