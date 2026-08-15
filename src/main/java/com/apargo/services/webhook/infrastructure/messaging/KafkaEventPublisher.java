package com.apargo.services.webhook.infrastructure.messaging;

import com.apargo.services.webhook.application.mapper.WebhookEventMapper;
import com.apargo.services.webhook.application.port.out.EventPublisherPort;
import com.apargo.services.webhook.domain.model.WebhookEvent;
import com.apargo.services.webhook.infrastructure.metrics.WebhookMetrics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes a stored event to its resolved topic under its resolved partition key.
 *
 * <p>Only ever called from the relay. Publishing inline on the request path would turn a broker blip
 * into a Meta unsubscribe, which is exactly what the outbox exists to prevent.
 *
 * <p>The envelope is serialised here rather than by a typed serialiser, so the wire format is a plain
 * JSON object with no framework type headers — consumers in other services parse it with their own
 * DTOs and are not coupled to this service's classes.
 */
@Component
public class KafkaEventPublisher implements EventPublisherPort {

    private static final String HEADER_EVENT_ID = "eventId";
    private static final String HEADER_LANE = "lane";
    private static final String HEADER_FIELD = "field";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final WebhookEventMapper mapper;
    private final WebhookMetrics metrics;

    public KafkaEventPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            WebhookEventMapper mapper,
            WebhookMetrics metrics) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.mapper = mapper;
        this.metrics = metrics;
    }

    @Override
    public CompletableFuture<Void> publish(WebhookEvent event) {
        String body;
        try {
            body = objectMapper.writeValueAsString(mapper.toEnvelope(event));
        } catch (JsonProcessingException e) {
            // Not retryable: the same document will fail identically on every attempt. Returning a
            // failed future lets the relay exhaust its attempts and land it in FAILED, where it is
            // visible in the support list instead of silently spinning.
            return CompletableFuture.failedFuture(e);
        }

        ProducerRecord<String, String> record =
                new ProducerRecord<>(event.topic(), event.partitionKey(), body);
        addHeader(record, HEADER_EVENT_ID, event.id());
        addHeader(record, HEADER_LANE, event.lane() == null ? null : event.lane().name());
        addHeader(record, HEADER_FIELD, event.field());

        return kafkaTemplate.send(record).thenAccept(result -> metrics.recordPublished(event.lane()));
    }

    private void addHeader(ProducerRecord<String, String> record, String key, String value) {
        if (value != null) {
            record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
