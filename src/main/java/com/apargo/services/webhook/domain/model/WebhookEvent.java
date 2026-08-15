package com.apargo.services.webhook.domain.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import lombok.Builder;

/**
 * One Meta {@code change}, stored durably before Meta is acknowledged.
 *
 * <p>{@link #payload()} is the raw change object exactly as received. It is never normalised,
 * reshaped or stripped: the moment this service takes a position on payload structure, it needs
 * changing every time Meta does, and it sits on the critical path for every service on the platform.
 *
 * @param id                    Mongo {@code _id} as a string; the correlation key across services
 * @param receivedAt            server receive time, not Meta's timestamp
 * @param bodyHash              hex SHA-256 of the raw POST body this change arrived in
 * @param field                 the Meta routing key, e.g. {@code messages}
 * @param lane                  the classified destination lane
 * @param topic                 resolved Kafka topic, denormalised so the relay does no lookup
 * @param partitionKey          resolved partition key, denormalised for the same reason
 * @param providerWabaId        Meta's WABA id, stored exactly as received and never resolved here
 * @param providerPhoneNumberId Meta's business phone number id, likewise
 * @param wamids                cheap extract for support search; never used for routing
 * @param eventCount            messages[] + statuses[] length
 * @param payload               the raw change object, untouched
 * @param truncated             true when the body exceeded the configured ceiling and was clipped
 */
@Builder(toBuilder = true)
public record WebhookEvent(

        String id,
        Instant receivedAt,
        String bodyHash,

        String field,
        Lane lane,
        String topic,
        String partitionKey,

        String providerWabaId,
        String providerPhoneNumberId,

        List<String> wamids,
        int eventCount,

        Map<String, Object> payload,
        boolean truncated,

        EventState state,
        int attempts,
        Instant nextAttemptAt,
        Instant leaseUntil,
        String lastError,
        Instant publishedAt) {

    public WebhookEvent {
        wamids = wamids == null ? List.of() : List.copyOf(wamids);
    }

    /** True when the relay has exhausted its attempts and an operator must intervene. */
    public boolean isTerminal() {
        return state == EventState.PUBLISHED || state == EventState.FAILED;
    }
}
