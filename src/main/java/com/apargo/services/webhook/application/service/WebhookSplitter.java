package com.apargo.services.webhook.application.service;

import com.apargo.services.webhook.application.port.out.TopicResolverPort;
import com.apargo.services.webhook.domain.exception.UnparseablePayloadException;
import com.apargo.services.webhook.domain.model.EventState;
import com.apargo.services.webhook.domain.model.Lane;
import com.apargo.services.webhook.domain.model.MetaField;
import com.apargo.services.webhook.domain.model.WebhookEvent;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Component;

/**
 * Turns one Meta POST into one document per {@code change}.
 *
 * <p>{@code changes} is the split boundary because it is the only level at which {@code field} and
 * {@code phone_number_id} are both singular. One POST may carry several entries and several changes
 * per entry.
 *
 * <p>The change object is copied into {@code payload} untouched. No normalisation, no reshaping, no
 * stripping — the splitter takes no position on what a payload means, which is what keeps this
 * service off the maintenance path every time Meta changes a field.
 */
@Component
public class WebhookSplitter {

    private static final TypeReference<Map<String, Object>> PAYLOAD_TYPE = new TypeReference<>() {
    };
    private static final String TRUNCATED_FIELD = "__oversized__";
    private static final String TRUNCATED_PAYLOAD_KEY = "rawBodyPrefix";

    private final ObjectMapper objectMapper;
    private final LaneClassifier laneClassifier;
    private final PartitionKeyResolver partitionKeyResolver;
    private final TopicResolverPort topicResolver;

    public WebhookSplitter(
            ObjectMapper objectMapper,
            LaneClassifier laneClassifier,
            PartitionKeyResolver partitionKeyResolver,
            TopicResolverPort topicResolver) {
        this.objectMapper = objectMapper;
        this.laneClassifier = laneClassifier;
        this.partitionKeyResolver = partitionKeyResolver;
        this.topicResolver = topicResolver;
    }

    /**
     * @param rawBody    the exact bytes received
     * @param bodyHash   hex SHA-256 of those bytes
     * @param receivedAt server receive time
     * @return one event per change per lane, ready to insert. Empty when the payload carries no
     *         changes at all, which is valid and answered 200.
     * @throws UnparseablePayloadException when the body is not JSON this service can walk
     */
    public List<WebhookEvent> split(byte[] rawBody, String bodyHash, Instant receivedAt) {
        JsonNode root = parse(rawBody);
        List<WebhookEvent> events = new ArrayList<>();

        for (JsonNode entry : JsonNodes.array(root, "entry")) {
            String providerWabaId = JsonNodes.text(entry, "id");

            for (JsonNode change : JsonNodes.array(entry, "changes")) {
                String field = JsonNodes.text(change, "field");
                JsonNode value = change.path("value");
                String providerPhoneNumberId =
                        JsonNodes.text(value.path("metadata"), "phone_number_id");

                Map<String, Object> payload = toPayload(change);
                List<String> wamids = collectWamids(value);
                int eventCount = countEvents(field, value);

                for (Lane lane : laneClassifier.classify(field, value)) {
                    events.add(WebhookEvent.builder()
                            .receivedAt(receivedAt)
                            .bodyHash(bodyHash)
                            .field(field)
                            .lane(lane)
                            .topic(topicResolver.resolve(lane))
                            .partitionKey(partitionKeyResolver.resolve(
                                    lane, value, providerWabaId, providerPhoneNumberId))
                            .providerWabaId(providerWabaId)
                            .providerPhoneNumberId(providerPhoneNumberId)
                            .wamids(wamids)
                            .eventCount(eventCount)
                            .payload(payload)
                            .truncated(false)
                            .state(EventState.PENDING)
                            .attempts(0)
                            .nextAttemptAt(receivedAt)
                            .build());
                }
            }
        }
        return events;
    }

    /**
     * Builds the single document stored when a body exceeds the configured ceiling. Meta documents
     * 3 MB as the maximum and this has never been observed, but losing an event that large would be
     * unrecoverable, so it is stored clipped and flagged rather than dropped.
     */
    public WebhookEvent truncatedEvent(byte[] rawBody, String bodyHash, Instant receivedAt, int keepBytes) {
        int length = Math.min(rawBody.length, Math.max(0, keepBytes));
        String prefix = new String(rawBody, 0, length, StandardCharsets.UTF_8);

        return WebhookEvent.builder()
                .receivedAt(receivedAt)
                .bodyHash(bodyHash)
                .field(TRUNCATED_FIELD)
                .lane(Lane.OTHER)
                .topic(topicResolver.resolve(Lane.OTHER))
                .partitionKey(bodyHash)
                .wamids(List.of())
                .eventCount(0)
                .payload(Map.of(
                        TRUNCATED_PAYLOAD_KEY, prefix,
                        "originalSizeBytes", rawBody.length))
                .truncated(true)
                .state(EventState.PENDING)
                .attempts(0)
                .nextAttemptAt(receivedAt)
                .build();
    }

    private JsonNode parse(byte[] rawBody) {
        if (rawBody == null || rawBody.length == 0) {
            throw new UnparseablePayloadException("Webhook body was empty", null);
        }
        try {
            JsonNode root = objectMapper.readTree(rawBody);
            if (root == null || !root.isObject()) {
                throw new UnparseablePayloadException("Webhook body was not a JSON object", null);
            }
            return root;
        } catch (UnparseablePayloadException e) {
            throw e;
        } catch (Exception e) {
            throw new UnparseablePayloadException("Webhook body could not be parsed as JSON", e);
        }
    }

    private Map<String, Object> toPayload(JsonNode change) {
        return objectMapper.convertValue(change, PAYLOAD_TYPE);
    }

    /** Cheap extract for support search only. Never used for routing or business logic. */
    private List<String> collectWamids(JsonNode value) {
        Set<String> wamids = new LinkedHashSet<>();
        collectIds(JsonNodes.array(value, "messages"), wamids);
        collectIds(JsonNodes.array(value, "statuses"), wamids);
        return List.copyOf(wamids);
    }

    private void collectIds(JsonNode array, Set<String> sink) {
        for (JsonNode element : array) {
            String id = JsonNodes.text(element, "id");
            if (id != null) {
                sink.add(id);
            }
        }
    }

    private int countEvents(String field, JsonNode value) {
        if (!MetaField.MESSAGES.equals(field)) {
            return 1;
        }
        return JsonNodes.array(value, "messages").size() + JsonNodes.array(value, "statuses").size();
    }
}
