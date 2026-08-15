package com.apargo.services.webhook.api.v1.dto;

import com.apargo.services.webhook.domain.model.EventState;
import com.apargo.services.webhook.domain.model.Lane;
import com.apargo.services.webhook.domain.model.WebhookEvent;
import java.time.Instant;
import java.util.List;

/**
 * List view of a stored event. Carries no payload — a support list should be scannable, and payloads
 * contain customer phone numbers and message text.
 */
public record WebhookEventSummaryResponse(
        String id,
        Instant receivedAt,
        String field,
        Lane lane,
        String topic,
        String providerWabaId,
        String providerPhoneNumberId,
        List<String> wamids,
        int eventCount,
        boolean truncated,
        EventState state,
        int attempts,
        Instant nextAttemptAt,
        Instant publishedAt,
        String lastError) {

    public static WebhookEventSummaryResponse from(WebhookEvent event) {
        return new WebhookEventSummaryResponse(
                event.id(),
                event.receivedAt(),
                event.field(),
                event.lane(),
                event.topic(),
                event.providerWabaId(),
                event.providerPhoneNumberId(),
                event.wamids(),
                event.eventCount(),
                event.truncated(),
                event.state(),
                event.attempts(),
                event.nextAttemptAt(),
                event.publishedAt(),
                event.lastError());
    }
}
