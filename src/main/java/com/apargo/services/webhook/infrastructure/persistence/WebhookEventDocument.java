package com.apargo.services.webhook.infrastructure.persistence;

import com.apargo.services.webhook.domain.model.EventState;
import com.apargo.services.webhook.domain.model.Lane;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * Persistent form of a webhook event. One document per {@code change}.
 *
 * <p>{@link #payload} holds the raw change object. Nothing in this service reshapes it.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = WebhookEventDocument.COLLECTION)
public class WebhookEventDocument {

    public static final String COLLECTION = "webhook_events";

    @Id
    private String id;

    /** Server receive time, not Meta's timestamp. Drives the TTL index. */
    private Instant receivedAt;

    /** Hex SHA-256 of the raw POST body. */
    private String bodyHash;

    // --- Routing -----------------------------------------------------------
    @Field("field")
    private String field;

    private Lane lane;

    /** Resolved at split time so the relay does no lookup. */
    private String topic;

    /** Resolved at split time for the same reason. */
    private String partitionKey;

    // --- Provider identifiers, stored exactly as received, never resolved here
    private String providerWabaId;

    private String providerPhoneNumberId;

    // --- Cheap extracts for support search. Never used for routing.
    private List<String> wamids;

    private int eventCount;

    private Map<String, Object> payload;

    /** True when the body exceeded the ceiling and only a prefix could be kept. */
    private boolean truncated;

    // --- Relay state -------------------------------------------------------
    private EventState state;

    private int attempts;

    private Instant nextAttemptAt;

    /** Null unless {@link EventState#PUBLISHING}. */
    private Instant leaseUntil;

    private String lastError;

    private Instant publishedAt;

    // Field names used by the adapter's queries and updates, kept in one place so a rename cannot
    // silently break a query that referenced the string.
    public static final class Fields {
        public static final String ID = "_id";
        public static final String RECEIVED_AT = "receivedAt";
        public static final String BODY_HASH = "bodyHash";
        public static final String FIELD = "field";
        public static final String LANE = "lane";
        public static final String PROVIDER_PHONE_NUMBER_ID = "providerPhoneNumberId";
        public static final String WAMIDS = "wamids";
        public static final String STATE = "state";
        public static final String ATTEMPTS = "attempts";
        public static final String NEXT_ATTEMPT_AT = "nextAttemptAt";
        public static final String LEASE_UNTIL = "leaseUntil";
        public static final String LAST_ERROR = "lastError";
        public static final String PUBLISHED_AT = "publishedAt";

        private Fields() {
        }
    }
}
