package com.apargo.services.webhook.domain.exception;

/**
 * Base type for every failure this service raises deliberately, carrying a stable machine-readable
 * code so the API layer never has to switch on exception classes.
 */
public abstract class WebhookException extends RuntimeException {

    private final String code;

    protected WebhookException(String code, String message) {
        super(message);
        this.code = code;
    }

    protected WebhookException(String code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
