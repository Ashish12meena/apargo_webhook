package com.apargo.services.webhook.application.port.out;

import com.apargo.services.webhook.domain.model.EventSearchCriteria;
import com.apargo.services.webhook.domain.model.EventState;
import com.apargo.services.webhook.domain.model.PageResult;
import com.apargo.services.webhook.domain.model.WebhookEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** The durable event store. The only thing on the ingest path that is allowed to fail the request. */
public interface WebhookEventRepositoryPort {

    /**
     * Durably inserts a batch. Implementations must use {@code {w: "majority", j: true}} and an
     * unordered insert — this is the single most important line in the service.
     *
     * @return the same events, carrying their assigned ids
     */
    List<WebhookEvent> insertAll(List<WebhookEvent> events);

    /**
     * Atomically claims up to {@code batchSize} due events, moving them to {@code PUBLISHING} under a
     * lease. Claim-then-publish: multiple instances must never double-publish.
     */
    List<WebhookEvent> claimBatch(int batchSize, Duration lease, Instant now);

    void markPublished(String id, Instant publishedAt);

    void markForRetry(String id, int attempts, Instant nextAttemptAt, String lastError);

    void markFailed(String id, int attempts, String lastError);

    /** Returns {@code PUBLISHING} documents whose lease has expired to {@code PENDING}. */
    long reclaimExpiredLeases(Instant now);

    Optional<WebhookEvent> findById(String id);

    PageResult<WebhookEvent> search(EventSearchCriteria criteria);

    Optional<WebhookEvent> resetForReplay(String id, Instant now);

    long resetAllForReplay(EventSearchCriteria criteria, Instant now);

    long countByState(EventState state);

    /** Receive time of the oldest undelivered event, for the relay lag gauge. */
    Optional<Instant> oldestPendingReceivedAt();
}
