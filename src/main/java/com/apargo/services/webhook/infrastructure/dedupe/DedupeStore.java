package com.apargo.services.webhook.infrastructure.dedupe;

/** One place a body hash can be remembered. Implementations are ordered by preference, not by role. */
public interface DedupeStore {

    /**
     * @return true when this hash had not been seen before
     * @throws RuntimeException when the store is unreachable, so the caller can fall through
     */
    boolean markSeen(String bodyHash);

    /** Human readable name, used only in logs. */
    String name();
}
