package com.apargo.services.webhook.application.service;

import com.apargo.services.webhook.domain.model.Lane;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

/**
 * Derives the Kafka partition key for a change, once, at split time. The key is stored on the
 * document so the relay performs no parsing and no lookup.
 *
 * <ul>
 *   <li><b>INBOUND</b> {@code {phoneNumberId}:{from}} — a conversation is exactly (business number,
 *       customer), so all of one customer's messages are consumed in order by one thread.
 *   <li><b>STATUS</b> the wamid — every status for a message lands on the same partition, which
 *       removes the sent/delivered/read interleaving problem structurally.
 *   <li>Everything else keys on the most specific stable identifier available.
 * </ul>
 *
 * <p>A change may bundle statuses for several messages; the first wamid is used. Consumers still
 * need rank-based upgrade guards regardless, because Meta can skip {@code delivered} entirely.
 */
@Component
public class PartitionKeyResolver {

    public String resolve(Lane lane, JsonNode value, String providerWabaId, String providerPhoneNumberId) {
        String key = switch (lane) {
            case INBOUND -> compose(providerPhoneNumberId, firstOf(value, "messages", "from"));
            case STATUS -> firstOf(value, "statuses", "id");
            case TEMPLATE -> firstNonBlank(
                    JsonNodes.text(value, "message_template_id"),
                    JsonNodes.text(value, "message_template_name"),
                    providerWabaId);
            case ACCOUNT -> firstNonBlank(providerPhoneNumberId, providerWabaId);
            case USER_PREFERENCE ->
                    compose(providerPhoneNumberId, firstOf(value, "user_preferences", "wa_id"));
            case OTHER -> providerWabaId;
        };
        // A null key means round-robin, which loses per-entity ordering, so fall back as far as
        // anything stable exists before giving up.
        return firstNonBlank(key, providerPhoneNumberId, providerWabaId);
    }

    private String compose(String left, String right) {
        if (left == null && right == null) {
            return null;
        }
        return (left == null ? "" : left) + ":" + (right == null ? "" : right);
    }

    private String firstOf(JsonNode value, String arrayName, String fieldName) {
        if (value == null) {
            return null;
        }
        JsonNode array = value.path(arrayName);
        if (!array.isArray() || array.isEmpty()) {
            return null;
        }
        return JsonNodes.text(array.get(0), fieldName);
    }

    private String firstNonBlank(String... candidates) {
        for (String candidate : candidates) {
            if (candidate != null && !candidate.isBlank()) {
                return candidate;
            }
        }
        return null;
    }
}
