package com.apargo.services.webhook.application.service;

import com.apargo.services.webhook.application.port.out.EventPublisherPort;
import com.apargo.services.webhook.domain.model.WebhookEvent;
import com.apargo.services.webhook.infrastructure.config.WebhookProperties;
import com.apargo.services.webhook.infrastructure.metrics.WebhookMetrics;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Hands the batch that ingest just wrote straight to Kafka, without reading it back.
 *
 * <p>The batch is already in memory when Mongo acknowledges its insert. The old flow wrote it,
 * answered Meta, and then had the relay claim the same documents back out of the database one
 * {@code findAndModify} at a time — a round trip per event to recover data the process had never let
 * go of.
 *
 * <p>Ordering is not negotiable: Mongo acknowledges, Meta gets its 200, and only then does anything
 * reach Kafka. Publishing before the write is confirmed means that if the write then fails, events
 * are on the topic with no record of them, Meta retries, and Kafka sees them twice. Consumers dedupe
 * on wamid so it would be survivable — there is simply no reason to accept it.
 *
 * <p>This path is an optimisation and is allowed to give up. Everything it declines to send, or
 * fails to send, stays PENDING and is collected by the recovery worker once the grace window
 * expires. That is why there is no error handling here beyond a counter: the fallback is not a
 * catch block, it is a different component that runs anyway.
 */
@Slf4j
@Component
public class FastPathPublisher {

    private final EventPublisherPort publisher;
    private final PublishAckCollector ackCollector;
    private final WebhookMetrics metrics;

    private final boolean enabled;
    private final int maxInFlight;

    /** Handed off but not yet acknowledged by the broker. Bounded by {@link #maxInFlight}. */
    private final AtomicInteger inFlight = new AtomicInteger();

    public FastPathPublisher(
            EventPublisherPort publisher,
            PublishAckCollector ackCollector,
            WebhookMetrics metrics,
            WebhookProperties properties) {
        this.publisher = publisher;
        this.ackCollector = ackCollector;
        this.metrics = metrics;
        this.enabled = properties.publisher().fastPathEnabled();
        this.maxInFlight = properties.publisher().maxInFlight();
        metrics.bindFastPathInFlight(inFlight);
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Publishes an already-persisted batch.
     *
     * @param events events carrying their assigned ids, as returned by the insert
     * @return how many were handed to the producer; the remainder stay PENDING for the recovery
     *         worker
     */
    public int handOff(List<WebhookEvent> events) {
        if (!enabled || events == null || events.isEmpty()) {
            return 0;
        }

        int handed = 0;
        for (int i = 0; i < events.size(); i++) {
            WebhookEvent event = events.get(i);

            if (event.id() == null) {
                // No id means no way to mark it published afterwards. Leave it to the recovery path.
                continue;
            }
            if (!tryAcquireSlot()) {
                // Everything from here on, including this one, stays PENDING.
                int abandoned = events.size() - i;
                metrics.recordFastPathRejected(abandoned);
                log.warn("Fast path at its in-flight bound of {}; leaving {} event(s) PENDING for "
                                + "the recovery worker. This means Kafka is not keeping up.",
                        maxInFlight, abandoned);
                break;
            }
            if (send(event)) {
                handed++;
            }
        }

        if (handed > 0) {
            metrics.recordFastPathHandedOff(handed);
        }
        return handed;
    }

    /**
     * Reserves an in-flight slot, or refuses.
     *
     * <p>Without this bound a Kafka outage becomes an OutOfMemoryError: ingest keeps accepting
     * webhooks, every one of them is handed to a producer that cannot drain, and the buffered
     * records accumulate until the heap gives out. Refusing instead degrades into a path that
     * already exists and is already tested.
     */
    private boolean tryAcquireSlot() {
        if (inFlight.incrementAndGet() > maxInFlight) {
            inFlight.decrementAndGet();
            return false;
        }
        return true;
    }

    private boolean send(WebhookEvent event) {
        try {
            publisher.publish(event).whenComplete((ignored, error) -> {
                inFlight.decrementAndGet();
                if (error == null) {
                    // A queue append. Nothing on this thread touches the database — see
                    // PublishAckCollector for why that sentence is the whole point.
                    ackCollector.acknowledge(event.id());
                } else {
                    onSendFailed(event, error);
                }
            });
            return true;
        } catch (RuntimeException e) {
            // A synchronous throw from the producer, e.g. buffer exhaustion or metadata timeout.
            inFlight.decrementAndGet();
            onSendFailed(event, e);
            return false;
        }
    }

    /**
     * Records the failure and does nothing else, on purpose.
     *
     * <p>The document is still PENDING with {@code nextAttemptAt} set one grace window ahead, so the
     * recovery worker will find it, publish it, and apply backoff and attempt counting if that fails
     * too. Writing retry state from here would duplicate that logic and put a Mongo round trip back
     * on the sender thread, which is the exact bottleneck this design removed.
     */
    private void onSendFailed(WebhookEvent event, Throwable error) {
        metrics.recordFastPathFailure();
        log.debug("Fast-path publish failed for eventId={} lane={}; the recovery worker will "
                + "retry it: {}", event.id(), event.lane(), error.toString());
    }
}
