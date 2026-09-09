package com.apargo.services.webhook.infrastructure.metrics;

import com.apargo.services.webhook.domain.model.Lane;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * Every meter this service publishes, in one place. Names and tag values come from
 * {@link MetricNames}, because they are hard-coded into dashboards and alert rules in another
 * repository and a rename here blinds an alert rather than breaking a build.
 *
 * <p>The gauge worth alerting on is {@code webhook.relay.lag} — the age of the oldest undelivered
 * event — not queue depth. Depth spikes during a campaign burst are normal; a document sitting
 * unpublished for five minutes means the relay is stuck.
 *
 * <p>Two of the fast-path meters are early warnings rather than health checks.
 * {@code webhook.fastpath.rejected} is non-zero only when the in-flight bound has been hit, which
 * means Kafka is not keeping up and events are falling back to the recovery worker.
 * {@code webhook.ack.queue.depth} growing without bound means Mongo is not keeping up with the
 * broker — the opposite problem, and invisible from throughput alone.
 */
@Component
public class WebhookMetrics {

    private final MeterRegistry registry;

    private final Counter received;
    private final Counter duplicate;
    private final Timer ingestLatency;

    private final Counter fastPathHandedOff;
    private final Counter fastPathRejected;
    private final Counter fastPathFailed;
    private final Counter ackFlushed;

    private final AtomicLong pendingEvents = new AtomicLong();
    private final AtomicLong failedEvents = new AtomicLong();
    private final AtomicLong relayLagSeconds = new AtomicLong();
    private final AtomicLong ackQueueDepth = new AtomicLong();

    private final Map<String, Counter> rejected = new ConcurrentHashMap<>();
    private final Map<Lane, Counter> published = new ConcurrentHashMap<>();

    public WebhookMetrics(MeterRegistry registry) {
        this.registry = registry;

        this.received = Counter.builder(MetricNames.INGEST_RECEIVED)
                .description("Meta POSTs accepted for processing")
                .register(registry);
        this.duplicate = Counter.builder(MetricNames.INGEST_DUPLICATE)
                .description("POSTs suppressed because the body hash had been seen before")
                .register(registry);
        this.ingestLatency = Timer.builder(MetricNames.INGEST_LATENCY)
                .description("Time from request arrival to the durable write being acknowledged")
                .publishPercentileHistogram()
                .register(registry);

        this.fastPathHandedOff = Counter.builder(MetricNames.FASTPATH_HANDED_OFF)
                .description("Events handed to the producer directly from the ingest batch")
                .register(registry);
        this.fastPathRejected = Counter.builder(MetricNames.FASTPATH_REJECTED)
                .description("Events left PENDING because the in-flight bound was reached. "
                        + "Any sustained non-zero value means Kafka is behind.")
                .register(registry);
        this.fastPathFailed = Counter.builder(MetricNames.FASTPATH_FAILED)
                .description("Fast-path sends the broker rejected; the recovery worker retries them")
                .register(registry);
        this.ackFlushed = Counter.builder(MetricNames.ACK_FLUSHED)
                .description("Events marked PUBLISHED by the batched ack flusher")
                .register(registry);

        Gauge.builder(MetricNames.EVENTS_PENDING, pendingEvents, AtomicLong::doubleValue)
                .description("Events stored but not yet published")
                .register(registry);
        Gauge.builder(MetricNames.EVENTS_FAILED, failedEvents, AtomicLong::doubleValue)
                .description("Events past max relay attempts. Alert on any non-zero value.")
                .register(registry);
        Gauge.builder(MetricNames.RELAY_LAG, relayLagSeconds, AtomicLong::doubleValue)
                .description("Age in seconds of the oldest pending event")
                .baseUnit(MetricNames.UNIT_SECONDS)
                .register(registry);
        Gauge.builder(MetricNames.ACK_QUEUE_DEPTH, ackQueueDepth, AtomicLong::doubleValue)
                .description("Acknowledgements waiting to be marked PUBLISHED")
                .baseUnit(MetricNames.UNIT_EVENTS)
                .register(registry);
    }

    /**
     * Publishes the in-flight count as a gauge over the counter the fast path already maintains,
     * rather than mirroring it into a second variable that could drift from the real bound.
     */
    public void bindFastPathInFlight(AtomicInteger inFlight) {
        Gauge.builder(MetricNames.FASTPATH_IN_FLIGHT, inFlight, AtomicInteger::doubleValue)
                .description("Events handed to the producer and not yet acknowledged")
                .baseUnit(MetricNames.UNIT_EVENTS)
                .register(registry);
    }

    public void recordReceived() {
        received.increment();
    }

    public void recordDuplicate() {
        duplicate.increment();
    }

    /** @param reason a low-cardinality tag; use the {@code REASON_*} constants in {@link MetricNames} */
    public void recordRejected(String reason) {
        rejected.computeIfAbsent(reason, r -> Counter.builder(MetricNames.INGEST_REJECTED)
                        .description("POSTs answered with something other than 200")
                        .tag(MetricNames.TAG_REASON, r)
                        .register(registry))
                .increment();
    }

    public void recordPublished(Lane lane) {
        if (lane == null) {
            return;
        }
        published.computeIfAbsent(lane, l -> Counter.builder(MetricNames.RELAY_PUBLISHED)
                        .description("Events acknowledged by the broker, per lane")
                        .tag(MetricNames.TAG_LANE, l.name())
                        .register(registry))
                .increment();
    }

    public void recordFastPathHandedOff(int count) {
        fastPathHandedOff.increment(count);
    }

    public void recordFastPathRejected(int count) {
        fastPathRejected.increment(count);
    }

    public void recordFastPathFailure() {
        fastPathFailed.increment();
    }

    public void recordAckFlushed(int count) {
        ackFlushed.increment(count);
    }

    public void setAckQueueDepth(int depth) {
        ackQueueDepth.set(depth);
    }

    public Timer.Sample startIngestTimer() {
        return Timer.start(registry);
    }

    public void stopIngestTimer(Timer.Sample sample) {
        sample.stop(ingestLatency);
    }

    public void updateRelayGauges(long pending, long failed, long lagSeconds) {
        pendingEvents.set(pending);
        failedEvents.set(failed);
        relayLagSeconds.set(lagSeconds);
    }
}
