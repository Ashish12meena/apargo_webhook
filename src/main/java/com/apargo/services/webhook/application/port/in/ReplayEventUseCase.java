package com.apargo.services.webhook.application.port.in;

import com.apargo.services.webhook.domain.model.EventSearchCriteria;
import com.apargo.services.webhook.domain.model.WebhookEvent;

/** Returns stored events to the relay queue. Internal plane only. */
public interface ReplayEventUseCase {

    /**
     * Resets a single event to {@code PENDING} so the relay picks it up on its next poll.
     *
     * @throws com.apargo.services.webhook.domain.exception.EventNotFoundException when absent
     */
    WebhookEvent replay(String id);

    /**
     * Bulk replay by filter.
     *
     * @return the number of documents reset
     * @throws com.apargo.services.webhook.domain.exception.InvalidReplayRequestException
     *         when the criteria carry no bounded date range
     */
    long replayAll(EventSearchCriteria criteria);
}
