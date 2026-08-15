package com.apargo.services.webhook.api.support;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import org.slf4j.MDC;

/**
 * The single response envelope for the internal support plane.
 *
 * <p>Deliberately not used on the Meta-facing endpoints: Meta expects a bare 200 and a plain-text
 * challenge, and wrapping either would break the subscription.
 *
 * @param correlationId echoed so a caller can quote it when asking for help
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ApiResponse<T>(
        boolean success,
        T data,
        ApiError error,
        Instant timestamp,
        String correlationId) {

    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<>(
                true,
                data,
                null,
                Instant.now(),
                currentCorrelationId());
    }

    public static <T> ApiResponse<T> failure(ApiError error) {
        return new ApiResponse<>(
                false,
                null,
                error,
                Instant.now(),
                currentCorrelationId());
    }

    private static String currentCorrelationId() {
        return MDC.get(CorrelationIdFilter.CORRELATION_ID_KEY);
    }
}