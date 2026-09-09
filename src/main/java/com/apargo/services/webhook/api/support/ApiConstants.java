package com.apargo.services.webhook.api.support;

/**
 * Paths, headers and error codes for the HTTP surface.
 *
 * <p>The path constants matter more than they look. {@link #WEBHOOK_EVENTS_PATH} appears in three
 * places that must agree — the controller mapping, the internal API key filter's URL patterns, and
 * the deployment's ingress rules — and a mismatch between the first two leaves the support plane
 * unauthenticated on the open internet without any error to notice.
 */
public final class ApiConstants {

    // --- Paths -------------------------------------------------------------
    public static final String API_V1 = "/api/v1";

    /** Meta's callback URL. Authenticated by HMAC, never by header. */
    public static final String WEBHOOK_PATH = API_V1 + "/webhook";

    /** The internal support plane. Guarded by {@link InternalApiKeyFilter}. */
    public static final String WEBHOOK_EVENTS_PATH = API_V1 + "/webhook-events";

    /** Servlet URL patterns for the filter registration. Must cover the path above and below it. */
    public static final String WEBHOOK_EVENTS_ROOT_PATTERN = WEBHOOK_EVENTS_PATH;
    public static final String WEBHOOK_EVENTS_SUBTREE_PATTERN = WEBHOOK_EVENTS_PATH + "/*";
    public static final String ALL_PATHS_PATTERN = "/*";

    // --- Meta handshake query parameters -----------------------------------
    public static final String HUB_MODE = "hub.mode";
    public static final String HUB_VERIFY_TOKEN = "hub.verify_token";
    public static final String HUB_CHALLENGE = "hub.challenge";

    /** The only value Meta sends for {@code hub.mode}. */
    public static final String SUBSCRIBE_MODE = "subscribe";

    // --- Headers -----------------------------------------------------------
    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    public static final String CORRELATION_ID_MDC_KEY = "correlationId";

    // --- Error codes -------------------------------------------------------
    public static final String ERROR_VALIDATION_FAILED = "VALIDATION_FAILED";
    public static final String ERROR_BAD_REQUEST = "BAD_REQUEST";
    public static final String ERROR_UNAUTHORIZED = "UNAUTHORIZED";
    public static final String ERROR_NOT_FOUND = "NOT_FOUND";
    public static final String ERROR_STORE_UNAVAILABLE = "STORE_UNAVAILABLE";
    public static final String ERROR_INTERNAL = "INTERNAL_ERROR";

    private ApiConstants() {
    }
}
