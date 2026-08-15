package com.apargo.services.webhook.infrastructure.dedupe;

import com.apargo.services.webhook.infrastructure.persistence.DedupeDocument;
import java.time.Clock;
import java.time.Instant;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

/**
 * Durable dedupe for when Redis is unavailable.
 *
 * <p>The hash is the document {@code _id}, so uniqueness comes free from the primary key: the insert
 * either succeeds, or raises a duplicate key error which is itself the duplicate detection. A TTL
 * index expires entries after the configured window.
 */
@Component
public class MongoDedupeFallback implements DedupeStore {

    private final MongoTemplate mongoTemplate;
    private final Clock clock;

    public MongoDedupeFallback(MongoTemplate mongoTemplate, Clock clock) {
        this.mongoTemplate = mongoTemplate;
        this.clock = clock;
    }

    @Override
    public boolean markSeen(String bodyHash) {
        try {
            mongoTemplate.insert(new DedupeDocument(bodyHash, Instant.now(clock)));
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    @Override
    public String name() {
        return "mongo";
    }
}
