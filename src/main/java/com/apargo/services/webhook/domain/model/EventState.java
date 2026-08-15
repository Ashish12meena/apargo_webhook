package com.apargo.services.webhook.domain.model;

/**
 * Outbox state of a stored event.
 *
 * <pre>
 * PENDING ──claim──► PUBLISHING ──ack───► PUBLISHED
 *    ▲                    │
 *    │                    ├──nack, attempts &lt; max──► PENDING (with backoff)
 *    │                    └──nack, attempts &gt;= max─► FAILED
 *    └──lease expired─────┘
 * </pre>
 */
public enum EventState {

    /** Stored and awaiting relay. */
    PENDING,

    /** Claimed by a relay instance and held under a lease. */
    PUBLISHING,

    /** Acknowledged by Kafka. Terminal, barring a manual replay. */
    PUBLISHED,

    /** Retries exhausted. Terminal until an operator replays it. */
    FAILED
}
