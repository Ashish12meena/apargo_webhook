package com.apargo.services.webhook.domain.exception;

/** The request did not carry a valid Meta signature. Answered 403 with nothing written. */
public class InvalidSignatureException extends WebhookException {

    public InvalidSignatureException(String message) {
        super("INVALID_SIGNATURE", message);
    }
}
