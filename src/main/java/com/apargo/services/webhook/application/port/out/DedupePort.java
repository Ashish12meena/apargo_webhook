package com.apargo.services.webhook.application.port.out;

/**
 * Duplicate suppression by body hash.
 *
 * <p>Dedupe state is reconstructible — the worst case of losing it is one redundant reprocess, which
 * every consumer handles idempotently. So an implementation must never fail the request because its
 * backing store is unavailable: a duplicate costs one reprocess, a rejected webhook costs a Meta
 * retry.
 */
public interface DedupePort {

    /**
     * @param bodyHash hex SHA-256 of the raw POST body
     * @return true if this hash has not been seen before and processing should continue
     */
    boolean markSeen(String bodyHash);
}
