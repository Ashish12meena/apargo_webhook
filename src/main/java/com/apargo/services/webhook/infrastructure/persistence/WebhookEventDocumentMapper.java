package com.apargo.services.webhook.infrastructure.persistence;

import com.apargo.services.webhook.domain.model.WebhookEvent;
import org.springframework.stereotype.Component;

/** Translates between the domain model and its persistent form, in one direction each. */
@Component
public class WebhookEventDocumentMapper {

    public WebhookEventDocument toDocument(WebhookEvent event) {
        return WebhookEventDocument.builder()
                .id(event.id())
                .receivedAt(event.receivedAt())
                .bodyHash(event.bodyHash())
                .field(event.field())
                .lane(event.lane())
                .topic(event.topic())
                .partitionKey(event.partitionKey())
                .providerWabaId(event.providerWabaId())
                .providerPhoneNumberId(event.providerPhoneNumberId())
                .wamids(event.wamids())
                .eventCount(event.eventCount())
                .payload(event.payload())
                .truncated(event.truncated())
                .state(event.state())
                .attempts(event.attempts())
                .nextAttemptAt(event.nextAttemptAt())
                .leaseUntil(event.leaseUntil())
                .lastError(event.lastError())
                .publishedAt(event.publishedAt())
                .build();
    }

    public WebhookEvent toDomain(WebhookEventDocument document) {
        return WebhookEvent.builder()
                .id(document.getId())
                .receivedAt(document.getReceivedAt())
                .bodyHash(document.getBodyHash())
                .field(document.getField())
                .lane(document.getLane())
                .topic(document.getTopic())
                .partitionKey(document.getPartitionKey())
                .providerWabaId(document.getProviderWabaId())
                .providerPhoneNumberId(document.getProviderPhoneNumberId())
                .wamids(document.getWamids())
                .eventCount(document.getEventCount())
                .payload(document.getPayload())
                .truncated(document.isTruncated())
                .state(document.getState())
                .attempts(document.getAttempts())
                .nextAttemptAt(document.getNextAttemptAt())
                .leaseUntil(document.getLeaseUntil())
                .lastError(document.getLastError())
                .publishedAt(document.getPublishedAt())
                .build();
    }
}
