package com.apargo.services.webhook.domain.exception;

/** No stored event with the requested id. Answered 404 on the internal support plane. */
public class EventNotFoundException extends WebhookException {

    public EventNotFoundException(String id) {
        super("EVENT_NOT_FOUND", "No webhook event with id " + id);
    }
}
