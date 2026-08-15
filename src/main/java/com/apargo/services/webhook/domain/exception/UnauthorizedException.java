package com.apargo.services.webhook.domain.exception;

/** Missing or wrong internal API key on the support plane. */
public class UnauthorizedException extends WebhookException {

    public UnauthorizedException(String message) {
        super("UNAUTHORIZED", message);
    }
}
