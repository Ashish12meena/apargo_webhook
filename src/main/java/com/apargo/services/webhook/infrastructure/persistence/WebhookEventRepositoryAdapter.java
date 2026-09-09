package com.apargo.services.webhook.infrastructure.persistence;

import com.apargo.services.webhook.application.port.out.WebhookEventRepositoryPort;
import com.apargo.services.webhook.domain.model.EventSearchCriteria;
import com.apargo.services.webhook.domain.model.EventState;
import com.apargo.services.webhook.domain.model.PageResult;
import com.apargo.services.webhook.domain.model.WebhookEvent;
import com.apargo.services.webhook.infrastructure.config.MongoConfig;
import com.apargo.services.webhook.infrastructure.persistence.WebhookEventDocument.Fields;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.result.InsertManyResult;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

/** The Mongo adapter. Everything durability-critical in this service happens in here. */
@Repository
public class WebhookEventRepositoryAdapter implements WebhookEventRepositoryPort {

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
     *
     * <p>The inserted count is checked rather than assumed. A partial success answered with a 200 is
     * the one failure mode that loses events permanently — Meta offers no replay — and it is exactly
     * what an unordered bulk write produces when a single document is rejected.
     */
    @Override
    public List<WebhookEvent> insertAll(List<WebhookEvent> events) {
        if (events == null || events.isEmpty()) {
            return List.of();
        }

        List<Document> documents = events.stream().map(this::toBsonDocument).toList();

        InsertManyResult result = mongoTemplate.execute(WebhookEventDocument.COLLECTION,
                collection -> collection
                        .withWriteConcern(MongoConfig.DURABLE_WRITE_CONCERN)
                        .insertMany(documents, new InsertManyOptions().ordered(false)));

        assertAllInserted(result, documents.size());

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
     * Claims up to {@code batchSize} due events in a fixed number of round trips, whatever the batch
     * size is.
     *
     * <p>This used to be a loop of up to {@code batchSize} sequential {@code findAndModify} calls —
     * one network round trip per document, which is why the batch size was pinned at 100 and why
     * raising it made things worse rather than better. The claim is now: read the due ids, stamp
     * them with a token this worker generated, then read back exactly what the stamp landed on.
     *
     * <p>The token is what makes three cheap operations as safe as N expensive ones. The stamping
     * update repeats the {@code state = PENDING} predicate, so under contention two workers cannot
     * both win the same document; the loser's update simply matches fewer documents, and the final
     * read returns only what this worker actually took.
     */
    @Override
    public List<WebhookEvent> claimBatch(int batchSize, Duration lease, Instant now) {
        List<String> dueIds = findDueIds(batchSize, now);
        if (dueIds.isEmpty()) {
            return List.of();
        }

        String claimToken = UUID.randomUUID().toString();

        Query contested = new Query(Criteria.where(Fields.ID).in(dueIds)
                .and(Fields.STATE).is(EventState.PENDING)
                .and(Fields.NEXT_ATTEMPT_AT).lte(now));

        Update claim = new Update()
                .set(Fields.STATE, EventState.PUBLISHING)
                .set(Fields.LEASE_UNTIL, now.plus(lease))
                .set(Fields.CLAIM_TOKEN, claimToken);

        long won = mongoTemplate.updateMulti(contested, claim, WebhookEventDocument.class)
                .getModifiedCount();
        if (won == 0) {
            // Every candidate was taken by another instance between the read and the stamp.
            return List.of();
        }

        Query mine = new Query(Criteria.where(Fields.CLAIM_TOKEN).is(claimToken));
        return mongoTemplate.find(mine, WebhookEventDocument.class).stream()
                .map(mapper::toDomain)
                .toList();
    }

    /**
     * Reads the ids of due documents only, never their payloads.
     *
     * <p>The projection matters: the drain runs twice a second and the payload is by far the largest
     * field on the document. Pulling whole documents here and again after the claim would move the
     * batch over the wire twice.
     */
    private List<String> findDueIds(int batchSize, Instant now) {
        Query due = new Query(Criteria.where(Fields.STATE).is(EventState.PENDING)
                        .and(Fields.NEXT_ATTEMPT_AT).lte(now))
                .with(Sort.by(Sort.Direction.ASC, Fields.ID))
                .limit(batchSize);
        due.fields().include(Fields.ID);

        return mongoTemplate.find(due, WebhookEventDocument.class).stream()
                .map(WebhookEventDocument::getId)
                .filter(java.util.Objects::nonNull)
                .toList();
    }

    /**
     * Marks a whole batch PUBLISHED in one write.
     *
     * <p>The count that matters is not this method's efficiency but where it is called from: on the
     * ack flusher thread, never on the producer's sender thread. A per-document write there cost the
     * service a measured 32ms per event, and a bulk write there would have cost the same in bigger
     * units.
     */
    @Override
    public int markPublishedBatch(Collection<String> ids, Instant publishedAt) {
        if (ids == null || ids.isEmpty()) {
            return 0;
        }

        List<String> all = ids.stream().filter(java.util.Objects::nonNull).distinct().toList();
        int marked = 0;

        // Chunked so a single command can never approach Mongo's 16MB limit, however large a flush
        // the ack queue has accumulated during a Mongo outage.
        for (int start = 0; start < all.size(); start += PersistenceConstants.MAX_IDS_PER_BULK_UPDATE) {
            int end = Math.min(start + PersistenceConstants.MAX_IDS_PER_BULK_UPDATE, all.size());
            List<String> chunk = all.subList(start, end);

            Query query = new Query(Criteria.where(Fields.ID).in(chunk));
            marked += (int) mongoTemplate.updateMulti(query, publishedUpdate(publishedAt),
                    WebhookEventDocument.class).getModifiedCount();
        }
        return marked;
    }

    @Override
    public void markPublished(String id, Instant publishedAt) {
        updateById(id, publishedUpdate(publishedAt));
    }

    @Override
    public void markForRetry(String id, int attempts, Instant nextAttemptAt, String lastError) {
        updateById(id, new Update()
                .set(Fields.STATE, EventState.PENDING)
                .set(Fields.ATTEMPTS, attempts)
                .set(Fields.NEXT_ATTEMPT_AT, nextAttemptAt)
                .set(Fields.LAST_ERROR, truncate(lastError))
                .set(Fields.LEASE_UNTIL, null)
                .unset(Fields.CLAIM_TOKEN));
    }

    @Override
    public void markFailed(String id, int attempts, String lastError) {
        updateById(id, new Update()
                .set(Fields.STATE, EventState.FAILED)
                .set(Fields.ATTEMPTS, attempts)
                .set(Fields.LAST_ERROR, truncate(lastError))
                .set(Fields.LEASE_UNTIL, null)
                .unset(Fields.CLAIM_TOKEN));
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
                .set(Fields.NEXT_ATTEMPT_AT, now)
                .unset(Fields.CLAIM_TOKEN);

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

    private Update publishedUpdate(Instant publishedAt) {
        return new Update()
                .set(Fields.STATE, EventState.PUBLISHED)
                .set(Fields.PUBLISHED_AT, publishedAt)
                .set(Fields.LEASE_UNTIL, null)
                .set(Fields.LAST_ERROR, null)
                .unset(Fields.CLAIM_TOKEN);
    }

    private Update replayUpdate(Instant now) {
        return new Update()
                .set(Fields.STATE, EventState.PENDING)
                .set(Fields.ATTEMPTS, 0)
                .set(Fields.NEXT_ATTEMPT_AT, now)
                .set(Fields.LAST_ERROR, null)
                .set(Fields.LEASE_UNTIL, null)
                .set(Fields.PUBLISHED_AT, null)
                .unset(Fields.CLAIM_TOKEN);
    }

    private void assertAllInserted(InsertManyResult result, int expected) {
        if (result == null) {
            return;
        }
        int acknowledged = result.getInsertedIds() == null ? 0 : result.getInsertedIds().size();
        if (acknowledged != expected) {
            throw new IllegalStateException("Durable insert acknowledged " + acknowledged
                    + " of " + expected + " documents. Answering 200 for the missing ones would "
                    + "lose them permanently — Meta offers no replay.");
        }
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
        return error.length() <= PersistenceConstants.MAX_ERROR_LENGTH
                ? error
                : error.substring(0, PersistenceConstants.MAX_ERROR_LENGTH);
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
