package com.apargo.services.webhook.infrastructure.persistence;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Durable dedupe fallback, used when Redis is unavailable.
 *
 * <p>The body hash is the {@code _id}, which makes uniqueness free: the insert either succeeds, or
 * raises a duplicate key error which <em>is</em> the duplicate detection.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = DedupeDocument.COLLECTION)
public class DedupeDocument {

    public static final String COLLECTION = "webhook_dedupe";
    public static final String SEEN_AT = "seenAt";

    /** The body hash. */
    @Id
    private String id;

    private Instant seenAt;
}
