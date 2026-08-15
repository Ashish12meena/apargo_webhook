package com.apargo.services.webhook.application.service;

import com.apargo.services.webhook.domain.model.Lane;
import com.apargo.services.webhook.domain.model.MetaField;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * Maps a change's {@code field} and {@code value} onto one or more lanes.
 *
 * <p>The {@code messages} field carries two structurally different things — inbound messages and
 * delivery statuses — so classification is by which array is present, never by the field alone. A
 * change carrying both produces two lanes; that does not occur today, and costing two lines to
 * handle it is cheaper than discovering that it does.
 */
@Component
public class LaneClassifier {

    private static final String MESSAGES_ARRAY = "messages";
    private static final String STATUSES_ARRAY = "statuses";

    /**
     * @param field the change's routing key
     * @param value the change's value object, may be null or missing
     * @return one lane, or two for a change carrying both messages and statuses. Never empty.
     */
    public List<Lane> classify(String field, JsonNode value) {
        if (field == null || field.isBlank()) {
            return List.of(Lane.OTHER);
        }

        if (MetaField.MESSAGES.equals(field)) {
            return classifyMessages(value);
        }
        if (field.startsWith(MetaField.TEMPLATE_PREFIX)
                || MetaField.TEMPLATE_CATEGORY_UPDATE.equals(field)) {
            return List.of(Lane.TEMPLATE);
        }
        if (MetaField.USER_PREFERENCES.equals(field)) {
            return List.of(Lane.USER_PREFERENCE);
        }
        if (field.startsWith(MetaField.PHONE_NUMBER_PREFIX)
                || field.startsWith(MetaField.ACCOUNT_PREFIX)
                || MetaField.BUSINESS_CAPABILITY_UPDATE.equals(field)
                || MetaField.SECURITY.equals(field)) {
            return List.of(Lane.ACCOUNT);
        }
        return List.of(Lane.OTHER);
    }

    private List<Lane> classifyMessages(JsonNode value) {
        List<Lane> lanes = new ArrayList<>(2);
        if (hasEntries(value, MESSAGES_ARRAY)) {
            lanes.add(Lane.INBOUND);
        }
        if (hasEntries(value, STATUSES_ARRAY)) {
            lanes.add(Lane.STATUS);
        }
        // Account-level errors arrive under "messages" with neither array present.
        return lanes.isEmpty() ? List.of(Lane.OTHER) : List.copyOf(lanes);
    }

    private boolean hasEntries(JsonNode value, String arrayName) {
        if (value == null) {
            return false;
        }
        JsonNode array = value.path(arrayName);
        return array.isArray() && !array.isEmpty();
    }
}
