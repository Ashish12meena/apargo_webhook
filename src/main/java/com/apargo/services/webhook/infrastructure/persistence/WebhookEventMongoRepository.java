package com.apargo.services.webhook.infrastructure.persistence;

import com.apargo.services.webhook.domain.model.EventState;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

/**
 * Derived queries for the trivial reads. Anything needing an atomic claim, an explicit write concern
 * or a dynamic filter goes through {@code MongoTemplate} in the adapter instead.
 */
@Repository
public interface WebhookEventMongoRepository extends MongoRepository<WebhookEventDocument, String> {

    long countByState(EventState state);
}
