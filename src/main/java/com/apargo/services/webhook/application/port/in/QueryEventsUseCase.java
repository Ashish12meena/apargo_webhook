package com.apargo.services.webhook.application.port.in;

import com.apargo.services.webhook.domain.model.EventSearchCriteria;
import com.apargo.services.webhook.domain.model.PageResult;
import com.apargo.services.webhook.domain.model.WebhookEvent;

/** Support reads. Internal plane only. */
public interface QueryEventsUseCase {

    PageResult<WebhookEvent> search(EventSearchCriteria criteria);

    /**
     * @throws com.apargo.services.webhook.domain.exception.EventNotFoundException when absent
     */
    WebhookEvent getById(String id);
}
