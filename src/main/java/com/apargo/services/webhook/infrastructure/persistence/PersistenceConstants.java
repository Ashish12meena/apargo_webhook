package com.apargo.services.webhook.infrastructure.persistence;

/**
 * Fixed facts about the storage layer: index names, the Redis key namespace, and the bounds that
 * stop a stored value growing without limit.
 *
 * <p>Nothing here is configurable, and that is the point. An index name is a database-side identity
 * — change it and the next startup creates a second index with the same keys instead of recognising
 * the one already there — so it belongs in a constant, not in {@code application.yml} where an
 * environment could disagree with the one that built the collection.
 */
public final class PersistenceConstants {

    // --- Index names -------------------------------------------------------

    /** The query the recovery worker runs twice a second. */
    public static final String INDEX_RELAY_DRAIN = "relay_drain";

    /** Supports the second half of the token claim: find everything this worker just marked. */
    public static final String INDEX_CLAIM_TOKEN = "claim_token";

    public static final String INDEX_LEASE_RECLAIM = "lease_reclaim";

    /** Retention. This service is the platform's replay log, and this is how long it lasts. */
    public static final String INDEX_RECEIVED_AT_TTL = "received_at_ttl";

    public static final String INDEX_SUPPORT_BY_PHONE_NUMBER = "support_by_phone_number";
    public static final String INDEX_SUPPORT_BY_WAMID = "support_by_wamid";
    public static final String INDEX_BODY_HASH = "body_hash";
    public static final String INDEX_DEDUPE_TTL = "seen_at_ttl";

    // --- Redis -------------------------------------------------------------

    /** Namespace for dedupe keys. Redis holds dedupe state and nothing else — never payloads. */
    public static final String DEDUPE_KEY_PREFIX = "wh:dedupe:";

    /** Value written under a dedupe key. Only its existence matters. */
    public static final String DEDUPE_MARKER = "1";

    // --- Bounds ------------------------------------------------------------

    /**
     * A stack trace stored verbatim on every failed document would dwarf the payload it describes
     * and, at scale, the collection itself.
     */
    public static final int MAX_ERROR_LENGTH = 1000;

    /**
     * Ids per {@code updateMany} when marking a batch PUBLISHED. Mongo's limit is the 16MB BSON
     * document size; this is well inside it and keeps any single write bounded in time.
     */
    public static final int MAX_IDS_PER_BULK_UPDATE = 1000;

    private PersistenceConstants() {
    }
}
