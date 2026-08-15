package com.apargo.services.webhook.application.mapper;

import com.apargo.services.webhook.domain.model.EventEnvelope;
import com.apargo.services.webhook.domain.model.WebhookEvent;
import org.springframework.stereotype.Component;

/**
 * Maps a stored event onto the contract published to consumers.
 *
 * <p>The mapping is deliberately thin. {@code payload} crosses unchanged, because this service takes
 * no position on what a Meta payload means and normalising here would put it on the maintenance path
 * every time Meta changes a field.
 */
@Component
public class WebhookEventMapper {

    public EventEnvelope toEnvelope(WebhookEvent event) {
        return EventEnvelope.from(event);
    }
}
