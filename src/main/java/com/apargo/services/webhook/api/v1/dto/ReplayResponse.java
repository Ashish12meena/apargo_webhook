package com.apargo.services.webhook.api.v1.dto;

/**
 * Result of a replay.
 *
 * @param eventsReset how many documents were returned to the relay queue
 * @param message     what an operator should expect to happen next
 */
public record ReplayResponse(long eventsReset, String message) {

    public static ReplayResponse single(String eventId) {
        return new ReplayResponse(1,
                "Event " + eventId + " was reset to PENDING and will be picked up by the next relay poll");
    }

    public static ReplayResponse bulk(long count) {
        return new ReplayResponse(count,
                count + " event(s) were reset to PENDING and will be republished by the relay");
    }
}
