package com.apargo.services.webhook.domain.model;

/**
 * The destination lane of a single Meta {@code change}.
 *
 * <p>A lane is a routing decision and nothing more. It carries no opinion about what the payload
 * means — that belongs to the consuming domain service.
 */
public enum Lane {

    /** Inbound messages from a WhatsApp user. Low volume, latency critical. */
    INBOUND,

    /** Delivery receipts for outbound messages. High volume, latency tolerant. */
    STATUS,

    /** Template lifecycle events: status, quality, components, category. */
    TEMPLATE,

    /** WABA and phone number level account events. */
    ACCOUNT,

    /** Marketing opt-in / opt-out preference changes. */
    USER_PREFERENCE,

    /**
     * Anything unrecognised. Routed to the unrouted topic rather than dropped: an unknown field
     * means Meta added something, or a subscription was enabled that nobody planned for.
     */
    OTHER
}
