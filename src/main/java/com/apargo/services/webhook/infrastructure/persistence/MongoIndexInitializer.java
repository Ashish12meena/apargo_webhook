package com.apargo.services.webhook.infrastructure.persistence;

import com.apargo.services.webhook.infrastructure.config.WebhookProperties;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.stereotype.Component;

/**
 * Creates the indexes the service depends on, explicitly, at startup.
 *
 * <p>Spring Data's automatic index creation is switched off deliberately: the indexes here are
 * operationally significant — two of them keep the relay from table-scanning under load and two are
 * the retention policy — so they are declared where they can be read and reviewed, not inferred from
 * annotations.
 *
 * <p>Index creation is idempotent, but changing a TTL on an existing index is not: Mongo rejects a
 * conflicting definition. That is logged as a warning with the remedy rather than failing startup,
 * because a running service that ingests events matters more than a retention window that is a day
 * out of date.
 */
@Slf4j
@Component
public class MongoIndexInitializer implements ApplicationRunner {

    private final MongoTemplate mongoTemplate;
    private final WebhookProperties properties;

    public MongoIndexInitializer(MongoTemplate mongoTemplate, WebhookProperties properties) {
        this.mongoTemplate = mongoTemplate;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        IndexOperations events = mongoTemplate.indexOps(WebhookEventDocument.class);

        // Relay drain: the query the scheduler runs twice a second.
        ensure(events, new Index()
                .on(WebhookEventDocument.Fields.STATE, Sort.Direction.ASC)
                .on(WebhookEventDocument.Fields.NEXT_ATTEMPT_AT, Sort.Direction.ASC)
                .on(WebhookEventDocument.Fields.ID, Sort.Direction.ASC)
                .named("relay_drain"));

        // Lease reclaim sweep.
        ensure(events, new Index()
                .on(WebhookEventDocument.Fields.STATE, Sort.Direction.ASC)
                .on(WebhookEventDocument.Fields.LEASE_UNTIL, Sort.Direction.ASC)
                .named("lease_reclaim"));

        // Retention. This service is the platform's replay log, and this is how long it lasts.
        Duration eventTtl = properties.retention().eventTtl();
        ensure(events, new Index()
                .on(WebhookEventDocument.Fields.RECEIVED_AT, Sort.Direction.ASC)
                .expire(eventTtl)
                .named("received_at_ttl"));

        // Support search.
        ensure(events, new Index()
                .on(WebhookEventDocument.Fields.PROVIDER_PHONE_NUMBER_ID, Sort.Direction.ASC)
                .on(WebhookEventDocument.Fields.RECEIVED_AT, Sort.Direction.DESC)
                .named("support_by_phone_number"));

        ensure(events, new Index()
                .on(WebhookEventDocument.Fields.WAMIDS, Sort.Direction.ASC)
                .sparse()
                .named("support_by_wamid"));

        // Dedupe fallback lookups.
        ensure(events, new Index()
                .on(WebhookEventDocument.Fields.BODY_HASH, Sort.Direction.ASC)
                .named("body_hash"));

        ensure(mongoTemplate.indexOps(DedupeDocument.class), new Index()
                .on(DedupeDocument.SEEN_AT, Sort.Direction.ASC)
                .expire(properties.ingest().dedupeTtl())
                .named("seen_at_ttl"));

        log.info("Mongo indexes verified: events TTL {}d, dedupe TTL {}h",
                eventTtl.toDays(), properties.ingest().dedupeTtl().toHours());
    }

    private void ensure(IndexOperations indexOperations, Index index) {
        try {
            indexOperations.createIndex(index);
        } catch (DataAccessException e) {
            log.warn("Could not create index {} — it likely exists with different options. "
                            + "Drop the existing index to apply the new definition. Cause: {}",
                    index, e.getMessage());
        }
    }
}
