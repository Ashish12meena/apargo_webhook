package com.apargo.services.webhook.domain.exception;

/**
 * A replay was asked for that this service refuses to perform — most importantly an unbounded bulk
 * replay, which against the status topic is an incident and must not be reachable by forgetting a
 * parameter.
 */
public class InvalidReplayRequestException extends WebhookException {

    public InvalidReplayRequestException(String message) {
        super("INVALID_REPLAY_REQUEST", message);
    }
}
