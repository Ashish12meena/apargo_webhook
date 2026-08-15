package com.apargo.services.webhook.domain.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.Map;

/**
 * The contract published to Kafka. Consumers parse {@link #payload()} themselves; this service takes
 * no position on what it means.
 *
 * <p>{@link #eventId()} is the Mongo {@code _id} as a string. It joins a consumer's logs to this
 * service's store and to the replay endpoint, and consumers are asked to log it on every message.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EventEnvelope(
        String eventId,
        Instant receivedAt,
        String field,
        Lane lane,
        String providerWabaId,
        String providerPhoneNumberId,
        Map<String, Object> payload) {

    public static EventEnvelope from(WebhookEvent event) {
        return new EventEnvelope(
                event.id(),
                event.receivedAt(),
                event.field(),
                event.lane(),
                event.providerWabaId(),
                event.providerPhoneNumberId(),
                event.payload());
    }
}
