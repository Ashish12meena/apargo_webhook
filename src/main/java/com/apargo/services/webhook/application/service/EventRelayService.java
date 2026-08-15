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
 * The outbox relay: claim, publish, mark.
 *
 * <p>Claim-then-publish rather than publish-then-mark, for the same reason as any work queue —
 * several instances poll the same collection and must not double-publish. A claim moves the document
 * to {@code PUBLISHING} under a lease; if the instance dies mid-flight the lease expires and the
 * document is swept back to {@code PENDING}.
 *
 * <p>Publishing is asynchronous on purpose. The scheduler thread claims a batch, hands each document
 * to the producer and returns; the broker acknowledgement completes the state transition later. A
 * slow broker therefore delays delivery but never blocks the poll loop.
 */
@Slf4j
@Service
public class EventRelayService {

    private final WebhookEventRepositoryPort repository;
    private final EventPublisherPort publisher;
    private final RelayBackoff backoff;
    private final WebhookProperties.Relay relayProperties;
    private final Clock clock;

    public EventRelayService(
            WebhookEventRepositoryPort repository,
            EventPublisherPort publisher,
            RelayBackoff backoff,
            WebhookProperties properties,
            Clock clock) {
        this.repository = repository;
        this.publisher = publisher;
        this.backoff = backoff;
        this.relayProperties = properties.relay();
        this.clock = clock;
    }

    /**
     * Claims one batch of due events and starts publishing them.
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
        log.debug("Relay claimed {} event(s)", claimed.size());

        for (WebhookEvent event : claimed) {
            publish(event);
        }
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
                    onSuccess(event);
                } else {
                    onFailure(event, error);
                }
            });
        } catch (RuntimeException e) {
            // A synchronous failure inside the producer, e.g. buffer exhaustion.
            onFailure(event, e);
        }
    }

    private void onSuccess(WebhookEvent event) {
        repository.markPublished(event.id(), Instant.now(clock));
        log.info("Published eventId={} lane={} topic={} partitionKey={}",
                event.id(), event.lane(), event.topic(), event.partitionKey());
    }

    private void onFailure(WebhookEvent event, Throwable error) {
        int attempts = event.attempts() + 1;
        String message = describe(error);

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
    }

    private String describe(Throwable error) {
        Throwable cause = error.getCause() == null ? error : error.getCause();
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }
}
