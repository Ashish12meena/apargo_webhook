package com.apargo.services.webhook.application.service;

import com.apargo.services.webhook.application.port.in.QueryEventsUseCase;
import com.apargo.services.webhook.application.port.in.ReplayEventUseCase;
import com.apargo.services.webhook.application.port.out.WebhookEventRepositoryPort;
import com.apargo.services.webhook.domain.exception.EventNotFoundException;
import com.apargo.services.webhook.domain.exception.InvalidReplayRequestException;
import com.apargo.services.webhook.domain.model.EventSearchCriteria;
import com.apargo.services.webhook.domain.model.PageResult;
import com.apargo.services.webhook.domain.model.WebhookEvent;
import java.time.Clock;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * The support plane: read what was received, and put it back on the queue.
 *
 * <p>These are the endpoints nobody needs until the first week a webhook fails in production, at
 * which point they are the difference between a diagnosis and a shrug.
 */
@Slf4j
@Service
public class WebhookEventQueryService implements QueryEventsUseCase, ReplayEventUseCase {

    private final WebhookEventRepositoryPort repository;
    private final Clock clock;

    public WebhookEventQueryService(WebhookEventRepositoryPort repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    @Override
    public PageResult<WebhookEvent> search(EventSearchCriteria criteria) {
        return repository.search(criteria);
    }

    @Override
    public WebhookEvent getById(String id) {
        return repository.findById(id).orElseThrow(() -> new EventNotFoundException(id));
    }

    @Override
    public WebhookEvent replay(String id) {
        WebhookEvent replayed = repository.resetForReplay(id, Instant.now(clock))
                .orElseThrow(() -> rejectSingleReplay(id));

        log.info("Replay requested for eventId={} lane={} topic={}",
                replayed.id(), replayed.lane(), replayed.topic());
        return replayed;
    }

    /**
     * Bulk replay demands an explicit date range. An unbounded replay of thirty days into the status
     * topic is an incident, and it must not be reachable by forgetting a parameter.
     */
    @Override
    public long replayAll(EventSearchCriteria criteria) {
        if (!criteria.hasBoundedRange()) {
            throw new InvalidReplayRequestException(
                    "Bulk replay requires both 'from' and 'to'. An unbounded replay would republish "
                            + "the entire retention window.");
        }
        if (criteria.from().isAfter(criteria.to())) {
            throw new InvalidReplayRequestException("'from' must be before 'to'");
        }

        long reset = repository.resetAllForReplay(criteria, Instant.now(clock));
        log.warn("Bulk replay reset {} event(s) between {} and {} (state={}, lane={})",
                reset, criteria.from(), criteria.to(), criteria.state(), criteria.lane());
        return reset;
    }

    /**
     * Distinguishes "no such event" from "that event is mid-flight", because the operator's next
     * action is different in each case.
     */
    private RuntimeException rejectSingleReplay(String id) {
        if (repository.findById(id).isEmpty()) {
            return new EventNotFoundException(id);
        }
        return new InvalidReplayRequestException(
                "Event " + id + " is currently being published. Wait for its lease to expire, then "
                        + "retry the replay.");
    }
}
