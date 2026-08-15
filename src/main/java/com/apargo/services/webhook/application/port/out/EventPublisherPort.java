package com.apargo.services.webhook.application.port.out;

import com.apargo.services.webhook.domain.model.WebhookEvent;
import java.util.concurrent.CompletableFuture;

/** Publishes a stored event to its resolved topic. Only ever called from the relay, never inline. */
public interface EventPublisherPort {

    /**
     * @return a future completing when the broker has acknowledged, or failing with the send error
     */
    CompletableFuture<Void> publish(WebhookEvent event);
}
