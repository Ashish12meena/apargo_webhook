package com.apargo.services.webhook.domain.model;

/**
 * Outcome of an ingest attempt. Every value here maps to HTTP 200 — the caller only ever answers
 * something else when the signature fails or the body cannot be parsed at all.
 *
 * @param outcome    what happened
 * @param storedCount number of documents written
 */
public record IngestResult(Outcome outcome, int storedCount) {

    public enum Outcome {
        /** Split and persisted. */
        STORED,
        /** Body hash already seen. Nothing written; Meta redelivers to every subscribed app. */
        DUPLICATE,
        /** Parsed, but contained no changes to store. */
        EMPTY,
        /** Over the configured ceiling. Stored truncated and flagged rather than lost. */
        TRUNCATED
    }

    public static IngestResult stored(int count) {
        return new IngestResult(Outcome.STORED, count);
    }

    public static IngestResult duplicate() {
        return new IngestResult(Outcome.DUPLICATE, 0);
    }

    public static IngestResult empty() {
        return new IngestResult(Outcome.EMPTY, 0);
    }

    public static IngestResult truncated() {
        return new IngestResult(Outcome.TRUNCATED, 1);
    }
}
