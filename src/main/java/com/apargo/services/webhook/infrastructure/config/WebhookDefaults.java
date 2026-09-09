package com.apargo.services.webhook.infrastructure.config;

/**
 * The default value of every tunable in the service, and why it is that value.
 *
 * <p>The division of labour is deliberate: {@link WebhookProperties} says what configuration
 * <em>exists</em> and what shape it has, this class says what the value is when nobody sets one, and
 * {@code application.yml} says which environment variable overrides it. A reader who wants to know
 * "what happens if I set nothing" reads this file and nothing else.
 *
 * <p>Every constant is a {@code String} because it is consumed by
 * {@link org.springframework.boot.context.properties.bind.DefaultValue}, whose value must be a
 * compile-time constant. Durations and data sizes therefore appear in Spring's own notation
 * ({@code 500ms}, {@code 3MB}) and are parsed by the binder, not here.
 */
public final class WebhookDefaults {

    // -----------------------------------------------------------------------
    // Meta
    // -----------------------------------------------------------------------

    /** Neither Meta secret has a default: a secret with a fallback value is a secret in git history. */
    public static final String META_SECRET = "";

    public static final String META_SIGNATURE_HEADER = "X-Hub-Signature-256";

    /** On by default. Disabling it is a local-development affordance, never a deployment option. */
    public static final String META_VERIFY_SIGNATURE = "true";

    // -----------------------------------------------------------------------
    // Scheduling
    // -----------------------------------------------------------------------

    /**
     * Must exceed the number of concurrent schedules. Spring's own default is a SINGLE thread: one
     * slow drain tick would stall lease reclaim and the relay-lag gauge along with it, so the one
     * metric that would tell you the relay is stuck stops updating exactly when the relay gets stuck.
     */
    public static final String SCHEDULING_POOL_SIZE = "4";

    // -----------------------------------------------------------------------
    // Ingest
    // -----------------------------------------------------------------------

    /** Meta's documented ceiling. A larger body is stored truncated and flagged, never dropped. */
    public static final String INGEST_MAX_PAYLOAD_SIZE = "3MB";

    /** Redis TTL and the Mongo TTL index on the dedupe collection. */
    public static final String INGEST_DEDUPE_TTL = "24h";

    /**
     * Off. {@code insertAll} is already one bulk write per request, so batching across requests
     * trades latency on the one path where Meta is waiting against a saving on the cheapest round
     * trip in the pipeline. The measured bottleneck was the relay, not ingest.
     */
    public static final String INGEST_BATCH_ENABLED = "false";

    /** Fires first under load; the timer then governs only light traffic. */
    public static final String INGEST_BATCH_MAX_RECORDS = "500";

    /** Measured as estimated serialised BSON, not raw request bytes. */
    public static final String INGEST_BATCH_MAX_BYTES = "8MB";

    /**
     * 50ms, not 500ms. The window governs only light traffic — precisely when batching buys nothing
     * and the latency is pure cost. At 500ms, p50 response time goes from a few milliseconds to
     * ~250ms and p99 past 500ms.
     */
    public static final String INGEST_BATCH_MAX_WAIT = "50ms";

    // -----------------------------------------------------------------------
    // Publisher (fast path)
    // -----------------------------------------------------------------------

    /** False routes every event through the recovery worker — correct, just slower. */
    public static final String PUBLISHER_FAST_PATH_ENABLED = "true";

    /**
     * Without a bound, a Kafka outage becomes an OOM as ingest keeps accepting and handoffs pile up.
     * Past the bound the fast path stops handing off and records simply stay PENDING, degrading into
     * a path that already exists.
     */
    public static final String PUBLISHER_MAX_IN_FLIGHT = "20000";

    public static final String PUBLISHER_ACK_FLUSH_SIZE = "1000";

    /** Must be less than or equal to {@link #RELAY_GRACE} / 3. Checked at startup. */
    public static final String PUBLISHER_ACK_FLUSH_INTERVAL = "500ms";

    // -----------------------------------------------------------------------
    // Relay (recovery worker)
    // -----------------------------------------------------------------------

    /** False and nothing polls the outbox. Events pile up in PENDING with no error logged anywhere. */
    public static final String RELAY_ENABLED = "true";

    public static final String RELAY_POLL_INTERVAL = "500ms";

    /**
     * Raised from 100. The claim is a fixed number of round trips regardless of batch size, so the
     * old value bought nothing and cost a poll cycle per hundred documents.
     */
    public static final String RELAY_BATCH_SIZE = "1000";

    /**
     * The most important value in this file. Every record is PENDING for the few hundred
     * milliseconds between its insert and its PUBLISHED mark, while the in-memory batch is already
     * in flight. A recovery worker with no age filter would find all of them and republish the
     * entire stream; the grace is what stops that.
     */
    public static final String RELAY_GRACE = "30s";

    public static final String RELAY_LEASE = "30s";

    public static final String RELAY_LEASE_RECLAIM_INTERVAL = "30s";

    /** Then FAILED, visible in the support list and replayable. */
    public static final String RELAY_MAX_ATTEMPTS = "10";

    public static final String RELAY_BACKOFF_BASE = "1s";

    /** Kafka outages are usually minutes and no user is waiting, so the schedule is patient. */
    public static final String RELAY_BACKOFF_MAX = "5m";

    /** Drives the pending, failed and lag gauges. */
    public static final String RELAY_METRICS_INTERVAL = "30s";

    // -----------------------------------------------------------------------
    // Topics
    // -----------------------------------------------------------------------

    public static final String TOPIC_INBOUND = "whatsapp.webhook.inbound";
    public static final String TOPIC_STATUS = "whatsapp.webhook.status";
    public static final String TOPIC_TEMPLATE = "whatsapp.webhook.template";
    public static final String TOPIC_ACCOUNT = "whatsapp.webhook.account";
    public static final String TOPIC_USER_PREFERENCE = "whatsapp.webhook.user-preference";
    public static final String TOPIC_UNROUTED = "whatsapp.webhook.unrouted";

    public static final String TOPIC_AUTO_CREATE = "true";
    public static final String TOPIC_REPLICAS = "1";

    // Partition counts follow from the keys: status is keyed by wamid and carries the most traffic,
    // inbound is keyed by conversation, and the unrouted dead letter needs exactly one.
    public static final String PARTITIONS_INBOUND = "6";
    public static final String PARTITIONS_STATUS = "12";
    public static final String PARTITIONS_TEMPLATE = "3";
    public static final String PARTITIONS_ACCOUNT = "3";
    public static final String PARTITIONS_USER_PREFERENCE = "3";
    public static final String PARTITIONS_UNROUTED = "1";

    // -----------------------------------------------------------------------
    // Retention and internal plane
    // -----------------------------------------------------------------------

    /** TTL index on {@code webhook_events.receivedAt}. This service is the platform's replay log. */
    public static final String RETENTION_EVENT_TTL = "30d";

    public static final String INTERNAL_API_KEY = "";
    public static final String INTERNAL_HEADER_NAME = "X-Internal-Api-Key";

    // -----------------------------------------------------------------------
    // Invariants
    // -----------------------------------------------------------------------

    /**
     * The ack flush interval must fit at least this many times inside the grace window. Three gives
     * a missed tick and a slow write room to recover before the recovery worker starts republishing
     * records the fast path has already delivered.
     */
    public static final int ACK_FLUSH_INTERVALS_PER_GRACE = 3;

    private WebhookDefaults() {
    }
}
