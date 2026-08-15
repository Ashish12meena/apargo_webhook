package com.apargo.services.webhook.api.v1.dto;

import com.apargo.services.webhook.domain.model.WebhookEvent;
import java.time.Instant;
import java.util.Map;

/**
 * Full view of a stored event, including the raw change exactly as Meta sent it.
 *
 * <p>This is the endpoint that makes the service a replay log rather than a black box.
 */
public record WebhookEventDetailResponse(
        WebhookEventSummaryResponse summary,
        String bodyHash,
        String partitionKey,
        Instant leaseUntil,
        Map<String, Object> payload) {

    public static WebhookEventDetailResponse from(WebhookEvent event) {
        return new WebhookEventDetailResponse(
                WebhookEventSummaryResponse.from(event),
                event.bodyHash(),
                event.partitionKey(),
                event.leaseUntil(),
                event.payload());
    }
}
