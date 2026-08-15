package com.apargo.services.webhook.infrastructure.metrics;

import com.apargo.services.webhook.domain.model.Lane;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * Every meter this service publishes, in one place.
 *
 * <p>The gauge worth alerting on is {@code webhook.relay.lag} — the age of the oldest undelivered
 * event — not queue depth. Depth spikes during a campaign burst are normal; a document sitting
 * unpublished for five minutes means the relay is stuck.
 */
@Component
public class WebhookMetrics {

    private static final String LANE_TAG = "lane";
    private static final String REASON_TAG = "reason";

    private final MeterRegistry registry;

    private final Counter received;
    private final Counter duplicate;
    private final Timer ingestLatency;

    private final AtomicLong pendingEvents = new AtomicLong();
    private final AtomicLong failedEvents = new AtomicLong();
    private final AtomicLong relayLagSeconds = new AtomicLong();

    private final Map<String, Counter> rejected = new ConcurrentHashMap<>();
    private final Map<Lane, Counter> published = new ConcurrentHashMap<>();

    public WebhookMetrics(MeterRegistry registry) {
        this.registry = registry;

        this.received = Counter.builder("webhook.ingest.received")
                .description("Meta POSTs accepted for processing")
                .register(registry);
        this.duplicate = Counter.builder("webhook.ingest.duplicate")
                .description("POSTs suppressed because the body hash had been seen before")
                .register(registry);
        this.ingestLatency = Timer.builder("webhook.ingest.latency")
                .description("Time from request arrival to the durable write being acknowledged")
                .publishPercentileHistogram()
                .register(registry);

        Gauge.builder("webhook.events.pending", pendingEvents, AtomicLong::doubleValue)
                .description("Events stored but not yet published")
                .register(registry);
        Gauge.builder("webhook.events.failed", failedEvents, AtomicLong::doubleValue)
                .description("Events past max relay attempts. Alert on any non-zero value.")
                .register(registry);
        Gauge.builder("webhook.relay.lag", relayLagSeconds, AtomicLong::doubleValue)
                .description("Age in seconds of the oldest pending event")
                .baseUnit("seconds")
                .register(registry);
    }

    public void recordReceived() {
        received.increment();
    }

    public void recordDuplicate() {
        duplicate.increment();
    }

    /** @param reason a low-cardinality tag such as {@code signature} or {@code unparseable} */
    public void recordRejected(String reason) {
        rejected.computeIfAbsent(reason, r -> Counter.builder("webhook.ingest.rejected")
                        .description("POSTs answered with something other than 200")
                        .tag(REASON_TAG, r)
                        .register(registry))
                .increment();
    }

    public void recordPublished(Lane lane) {
        published.computeIfAbsent(lane, l -> Counter.builder("webhook.relay.published")
                        .description("Events acknowledged by the broker, per lane")
                        .tag(LANE_TAG, l.name())
                        .register(registry))
                .increment();
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
