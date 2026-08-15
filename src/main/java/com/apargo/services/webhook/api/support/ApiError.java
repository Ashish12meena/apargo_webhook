package com.apargo.services.webhook.api.support;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;

/**
 * The error half of {@link ApiResponse}.
 *
 * @param code    stable machine-readable code, safe to switch on from a caller
 * @param message human-readable explanation, safe to show an operator
 * @param details optional field-level detail, present only for validation failures
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ApiError(String code, String message, Map<String, String> details) {

    public static ApiError of(String code, String message) {
        return new ApiError(code, message, null);
    }

    public static ApiError of(String code, String message, Map<String, String> details) {
        return new ApiError(code, message, details);
    }
}
