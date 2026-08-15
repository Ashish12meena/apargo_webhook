package com.apargo.services.webhook.domain.exception;

/**
 * The body is not JSON this service can walk at all. Answered 400: a Meta retry cannot fix a
 * malformed body, and the body itself is what is needed to diagnose it.
 */
public class UnparseablePayloadException extends WebhookException {

    public UnparseablePayloadException(String message, Throwable cause) {
        super("UNPARSEABLE_PAYLOAD", message, cause);
    }
}
