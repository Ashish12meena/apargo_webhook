package com.apargo.services.webhook.infrastructure.persistence;

import com.apargo.services.webhook.application.port.out.WebhookEventRepositoryPort;
import com.apargo.services.webhook.domain.model.EventSearchCriteria;
import com.apargo.services.webhook.domain.model.EventState;
import com.apargo.services.webhook.domain.model.PageResult;
import com.apargo.services.webhook.domain.model.WebhookEvent;
import com.apargo.services.webhook.infrastructure.config.MongoConfig;
import com.mongodb.client.model.InsertManyOptions;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import com.apargo.services.webhook.infrastructure.persistence.WebhookEventDocument.Fields;

/** The Mongo adapter. Everything durability-critical in this service happens in here. */
@Repository
public class WebhookEventRepositoryAdapter implements WebhookEventRepositoryPort {

    private static final int MAX_ERROR_LENGTH = 1000;

    private final MongoTemplate mongoTemplate;
    private final WebhookEventMongoRepository repository;
    private final WebhookEventDocumentMapper mapper;

    public WebhookEventRepositoryAdapter(
            MongoTemplate mongoTemplate,
            WebhookEventMongoRepository repository,
            WebhookEventDocumentMapper mapper) {
        this.mongoTemplate = mongoTemplate;
        this.repository = repository;
        this.mapper = mapper;
    }

    /**
     * The single most important operation in the service: one unordered {@code insertMany} carrying
     * an explicit {@code {w: "majority", j: true}}.
     *
     * <p>Written against the driver deliberately rather than through a repository save, so that the
     * write concern and the unordered flag are visible at the call site and cannot be silently lost
     * by a change elsewhere. {@code ordered:false} means one bad document cannot stop the rest of a
     * batch from landing.
     */
    @Override
    public List<WebhookEvent> insertAll(List<WebhookEvent> events) {
        if (events == null || events.isEmpty()) {
            return List.of();
        }

        List<Document> documents = events.stream().map(this::toBsonDocument).toList();

        mongoTemplate.execute(WebhookEventDocument.COLLECTION, collection -> collection
                .withWriteConcern(MongoConfig.DURABLE_WRITE_CONCERN)
                .insertMany(documents, new InsertManyOptions().ordered(false)));

        List<WebhookEvent> stored = new ArrayList<>(events.size());
        for (int i = 0; i < events.size(); i++) {
            Object assignedId = documents.get(i).get(Fields.ID);
            stored.add(events.get(i).toBuilder()
                    .id(assignedId == null ? null : assignedId.toString())
                    .build());
        }
        return stored;
    }

    /**
     * Claim-then-publish. Each document is moved to {@code PUBLISHING} under a lease by an atomic
     * {@code findAndModify}, so two instances polling the same batch cannot both take the same
     * document and double-publish it.
     */
    @Override
    public List<WebhookEvent> claimBatch(int batchSize, Duration lease, Instant now) {
        Query query = new Query(Criteria.where(Fields.STATE).is(EventState.PENDING)
                        .and(Fields.NEXT_ATTEMPT_AT).lte(now))
                .with(Sort.by(Sort.Direction.ASC, Fields.ID));

        FindAndModifyOptions options = FindAndModifyOptions.options().returnNew(true);

        List<WebhookEvent> claimed = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            Update update = new Update()
                    .set(Fields.STATE, EventState.PUBLISHING)
                    .set(Fields.LEASE_UNTIL, now.plus(lease));

            WebhookEventDocument document =
                    mongoTemplate.findAndModify(query, update, options, WebhookEventDocument.class);
            if (document == null) {
                break;
            }
            claimed.add(mapper.toDomain(document));
        }
        return claimed;
    }

    @Override
    public void markPublished(String id, Instant publishedAt) {
        updateById(id, new Update()
                .set(Fields.STATE, EventState.PUBLISHED)
                .set(Fields.PUBLISHED_AT, publishedAt)
                .set(Fields.LEASE_UNTIL, null)
                .set(Fields.LAST_ERROR, null));
    }

    @Override
    public void markForRetry(String id, int attempts, Instant nextAttemptAt, String lastError) {
        updateById(id, new Update()
                .set(Fields.STATE, EventState.PENDING)
                .set(Fields.ATTEMPTS, attempts)
                .set(Fields.NEXT_ATTEMPT_AT, nextAttemptAt)
                .set(Fields.LAST_ERROR, truncate(lastError))
                .set(Fields.LEASE_UNTIL, null));
    }

    @Override
    public void markFailed(String id, int attempts, String lastError) {
        updateById(id, new Update()
                .set(Fields.STATE, EventState.FAILED)
                .set(Fields.ATTEMPTS, attempts)
                .set(Fields.LAST_ERROR, truncate(lastError))
                .set(Fields.LEASE_UNTIL, null));
    }

    /**
     * A pod that died mid-publish leaves documents stranded in {@code PUBLISHING}. Once the lease has
     * expired they are safe to return to the queue; the consumer side is idempotent, so a republish
     * costs nothing.
     */
    @Override
    public long reclaimExpiredLeases(Instant now) {
        Query query = new Query(Criteria.where(Fields.STATE).is(EventState.PUBLISHING)
                .and(Fields.LEASE_UNTIL).lt(now));

        Update update = new Update()
                .set(Fields.STATE, EventState.PENDING)
                .set(Fields.LEASE_UNTIL, null)
                .set(Fields.NEXT_ATTEMPT_AT, now);

        return mongoTemplate.updateMulti(query, update, WebhookEventDocument.class).getModifiedCount();
    }

    @Override
    public Optional<WebhookEvent> findById(String id) {
        return repository.findById(id).map(mapper::toDomain);
    }

    @Override
    public PageResult<WebhookEvent> search(EventSearchCriteria criteria) {
        Query query = new Query(toCriteria(criteria));
        long total = mongoTemplate.count(query, WebhookEventDocument.class);

        query.with(Sort.by(Sort.Direction.DESC, Fields.RECEIVED_AT))
                .skip((long) criteria.page() * criteria.size())
                .limit(criteria.size());

        List<WebhookEvent> content = mongoTemplate.find(query, WebhookEventDocument.class).stream()
                .map(mapper::toDomain)
                .toList();

        return new PageResult<>(content, total, criteria.page(), criteria.size());
    }

    @Override
    public Optional<WebhookEvent> resetForReplay(String id, Instant now) {
        // A document under an active lease is mid-flight; replaying it would double-publish.
        Query query = new Query(Criteria.where(Fields.ID).is(id)
                .and(Fields.STATE).ne(EventState.PUBLISHING));

        WebhookEventDocument document = mongoTemplate.findAndModify(
                query,
                replayUpdate(now),
                FindAndModifyOptions.options().returnNew(true),
                WebhookEventDocument.class);

        return Optional.ofNullable(document).map(mapper::toDomain);
    }

    @Override
    public long resetAllForReplay(EventSearchCriteria criteria, Instant now) {
        Query query = new Query(toCriteria(criteria).and(Fields.STATE).ne(EventState.PUBLISHING));
        return mongoTemplate.updateMulti(query, replayUpdate(now), WebhookEventDocument.class)
                .getModifiedCount();
    }

    @Override
    public long countByState(EventState state) {
        return repository.countByState(state);
    }

    @Override
    public Optional<Instant> oldestPendingReceivedAt() {
        Query query = new Query(Criteria.where(Fields.STATE).is(EventState.PENDING))
                .with(Sort.by(Sort.Direction.ASC, Fields.RECEIVED_AT))
                .limit(1);

        return Optional.ofNullable(mongoTemplate.findOne(query, WebhookEventDocument.class))
                .map(WebhookEventDocument::getReceivedAt);
    }

    // -----------------------------------------------------------------------

    private Update replayUpdate(Instant now) {
        return new Update()
                .set(Fields.STATE, EventState.PENDING)
                .set(Fields.ATTEMPTS, 0)
                .set(Fields.NEXT_ATTEMPT_AT, now)
                .set(Fields.LAST_ERROR, null)
                .set(Fields.LEASE_UNTIL, null)
                .set(Fields.PUBLISHED_AT, null);
    }

    private Criteria toCriteria(EventSearchCriteria criteria) {
        List<Criteria> clauses = new ArrayList<>();

        if (criteria.state() != null) {
            clauses.add(Criteria.where(Fields.STATE).is(criteria.state()));
        }
        if (criteria.lane() != null) {
            clauses.add(Criteria.where(Fields.LANE).is(criteria.lane()));
        }
        if (hasText(criteria.field())) {
            clauses.add(Criteria.where(Fields.FIELD).is(criteria.field()));
        }
        if (hasText(criteria.providerPhoneNumberId())) {
            clauses.add(Criteria.where(Fields.PROVIDER_PHONE_NUMBER_ID).is(criteria.providerPhoneNumberId()));
        }
        if (hasText(criteria.wamid())) {
            clauses.add(Criteria.where(Fields.WAMIDS).is(criteria.wamid()));
        }
        if (criteria.from() != null) {
            clauses.add(Criteria.where(Fields.RECEIVED_AT).gte(criteria.from()));
        }
        if (criteria.to() != null) {
            clauses.add(Criteria.where(Fields.RECEIVED_AT).lt(criteria.to()));
        }

        return clauses.isEmpty() ? new Criteria() : new Criteria().andOperator(clauses);
    }

    private void updateById(String id, Update update) {
        mongoTemplate.updateFirst(
                new Query(Criteria.where(Fields.ID).is(id)), update, WebhookEventDocument.class);
    }

    private Document toBsonDocument(WebhookEvent event) {
        Document sink = new Document();
        mongoTemplate.getConverter().write(mapper.toDocument(event), sink);
        return sink;
    }

    private String truncate(String error) {
        if (error == null) {
            return null;
        }
        return error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH);
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
