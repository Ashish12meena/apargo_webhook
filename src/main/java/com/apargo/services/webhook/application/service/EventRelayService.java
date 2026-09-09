package com.apargo.services.webhook.application.service;

import com.apargo.services.webhook.application.port.out.EventPublisherPort;
import com.apargo.services.webhook.application.port.out.WebhookEventRepositoryPort;
import com.apargo.services.webhook.domain.model.WebhookEvent;
import com.apargo.services.webhook.domain.policy.RelayBackoff;
import com.apargo.services.webhook.infrastructure.config.WebhookProperties;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * The recovery worker: claim, publish, mark.
 *
 * <p>Once the fast path is enabled this is no longer the pipeline. It is the thing that catches what
 * the fast path dropped — a crash between the insert and the handoff, a Kafka outage that hit the
 * in-flight bound, a send that failed. On a healthy service it should mostly claim nothing, and a
 * steadily non-zero claim rate is a signal worth investigating rather than normal operation.
 *
 * <p>Claim-then-publish rather than publish-then-mark, for the same reason as any work queue —
 * several instances poll the same collection and must not double-publish. A claim moves the document
 * to {@code PUBLISHING} under a lease; if the instance dies mid-flight the lease expires and the
 * document is swept back to {@code PENDING}.
 *
 * <p>Neither callback below touches the database. Broker confirmations all arrive on the
 * {@code KafkaProducer}'s single sender thread, and a Mongo round trip there serialises the entire
 * service behind one thread — measured at 32ms per event, which is 30 events/sec no matter how much
 * traffic arrives. Successes go to {@link PublishAckCollector} as a queue append; failures are
 * handed to the same collector's thread to be written from there.
 */
@Slf4j
@Service
public class EventRelayService {

    private final WebhookEventRepositoryPort repository;
    private final EventPublisherPort publisher;
    private final PublishAckCollector ackCollector;
    private final RelayBackoff backoff;
    private final WebhookProperties.Relay relayProperties;
    private final Clock clock;

    public EventRelayService(
            WebhookEventRepositoryPort repository,
            EventPublisherPort publisher,
            PublishAckCollector ackCollector,
            RelayBackoff backoff,
            WebhookProperties properties,
            Clock clock) {
        this.repository = repository;
        this.publisher = publisher;
        this.ackCollector = ackCollector;
        this.backoff = backoff;
        this.relayProperties = properties.relay();
        this.clock = clock;
    }

    /**
     * Claims one batch of due events and starts publishing them.
     *
     * <p>Only records whose {@code nextAttemptAt} has passed are eligible, which is what keeps this
     * worker away from the records the fast path is currently delivering: they are inserted one
     * grace window in the future and are marked PUBLISHED long before it elapses. Removing the grace
     * makes this method find the entire live stream and republish all of it.
     *
     * @return the number of events claimed
     */
    public int drainOnce() {
        Instant now = Instant.now(clock);
        List<WebhookEvent> claimed =
                repository.claimBatch(relayProperties.batchSize(), relayProperties.lease(), now);

        if (claimed.isEmpty()) {
            return 0;
        }

        for (WebhookEvent event : claimed) {
            publish(event);
        }

        // One line per batch, not per event. Two INFO lines per event appended synchronously to the
        // journal are free at 30 events/sec and are not at 2,000.
        log.info("Recovery worker claimed and published {} event(s)", claimed.size());
        return claimed.size();
    }

    /** Returns documents stranded in {@code PUBLISHING} by a dead instance back to the queue. */
    public long reclaimExpiredLeases() {
        long reclaimed = repository.reclaimExpiredLeases(Instant.now(clock));
        if (reclaimed > 0) {
            log.warn("Reclaimed {} event(s) whose publish lease had expired", reclaimed);
        }
        return reclaimed;
    }

    private void publish(WebhookEvent event) {
        try {
            publisher.publish(event).whenComplete((ignored, error) -> {
                if (error == null) {
                    // Queue append only. See the class javadoc for why this is not a write.
                    ackCollector.acknowledge(event.id());
                } else {
                    // Off the sender thread before anything touches Mongo. Failures are rare enough
                    // to share the flusher thread rather than justify one of their own.
                    ackCollector.submit(() -> onFailure(event, error));
                }
            });
        } catch (RuntimeException e) {
            // A synchronous failure inside the producer, e.g. buffer exhaustion. This runs on the
            // scheduler thread, not a producer callback, so handling it inline is fine.
            onFailure(event, e);
        }
    }

    private void onFailure(WebhookEvent event, Throwable error) {
        int attempts = event.attempts() + 1;
        String message = describe(error);

        try {
            if (backoff.isExhausted(attempts)) {
                repository.markFailed(event.id(), attempts, message);
                log.error("Relay gave up on eventId={} lane={} topic={} after {} attempts. "
                                + "The event is stored and replayable via POST /api/v1/webhook-events/{}/replay",
                        event.id(), event.lane(), event.topic(), attempts, event.id(), error);
                return;
            }

            Instant nextAttemptAt = Instant.now(clock).plus(backoff.delayFor(attempts));
            repository.markForRetry(event.id(), attempts, nextAttemptAt, message);
            log.warn("Publish failed for eventId={} lane={} attempt={}, retrying at {}: {}",
                    event.id(), event.lane(), attempts, nextAttemptAt, message);

        } catch (RuntimeException e) {
            // The document keeps its lease and is reclaimed once it expires, so the event is not
            // lost. Rethrowing would kill the flusher thread and take the ack path with it.
            log.error("Could not record the publish failure for eventId={}; it will be reclaimed "
                    + "when its lease expires", event.id(), e);
        }
    }

    private String describe(Throwable error) {
        Throwable cause = error.getCause() == null ? error : error.getCause();
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }
}
