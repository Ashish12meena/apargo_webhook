package com.apargo.services.webhook.api.support;

import com.apargo.services.webhook.domain.exception.EventNotFoundException;
import com.apargo.services.webhook.domain.exception.InvalidReplayRequestException;
import com.apargo.services.webhook.domain.exception.InvalidSignatureException;
import com.apargo.services.webhook.domain.exception.UnauthorizedException;
import com.apargo.services.webhook.domain.exception.UnparseablePayloadException;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/**
 * One place where an exception becomes an HTTP response.
 *
 * <p>This advice serves the internal support plane. The Meta-facing controller handles its own
 * failures inline and never reaches here, because its status codes are dictated by Meta's retry
 * behaviour rather than by ordinary REST conventions.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(EventNotFoundException.class)
    public ResponseEntity<ApiResponse<Void>> handleNotFound(EventNotFoundException e) {
        return respond(HttpStatus.NOT_FOUND, e.getCode(), e.getMessage());
    }

    @ExceptionHandler(InvalidReplayRequestException.class)
    public ResponseEntity<ApiResponse<Void>> handleInvalidReplay(InvalidReplayRequestException e) {
        return respond(HttpStatus.BAD_REQUEST, e.getCode(), e.getMessage());
    }

    @ExceptionHandler(UnauthorizedException.class)
    public ResponseEntity<ApiResponse<Void>> handleUnauthorized(UnauthorizedException e) {
        return respond(HttpStatus.UNAUTHORIZED, e.getCode(), e.getMessage());
    }

    /** Only reachable if a signature failure escapes the webhook controller. */
    @ExceptionHandler(InvalidSignatureException.class)
    public ResponseEntity<ApiResponse<Void>> handleInvalidSignature(InvalidSignatureException e) {
        return respond(HttpStatus.FORBIDDEN, e.getCode(), e.getMessage());
    }

    /** Likewise for an unparseable body. */
    @ExceptionHandler(UnparseablePayloadException.class)
    public ResponseEntity<ApiResponse<Void>> handleUnparseable(UnparseablePayloadException e) {
        return respond(HttpStatus.BAD_REQUEST, e.getCode(), e.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiResponse<Void>> handleValidation(MethodArgumentNotValidException e) {
        Map<String, String> details = new LinkedHashMap<>();
        for (FieldError fieldError : e.getBindingResult().getFieldErrors()) {
            details.put(fieldError.getField(), fieldError.getDefaultMessage());
        }
        return ResponseEntity.badRequest().body(ApiResponse.failure(
                ApiError.of("VALIDATION_FAILED", "The request body is not valid", details)));
    }

    @ExceptionHandler({
            MethodArgumentTypeMismatchException.class,
            MissingServletRequestParameterException.class,
            IllegalArgumentException.class})
    public ResponseEntity<ApiResponse<Void>> handleBadRequest(Exception e) {
        return respond(HttpStatus.BAD_REQUEST, "BAD_REQUEST", e.getMessage());
    }

    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<ApiResponse<Void>> handleDataAccess(DataAccessException e) {
        log.error("Data store unavailable while serving a support request", e);
        return respond(HttpStatus.SERVICE_UNAVAILABLE, "STORE_UNAVAILABLE",
                "The event store is currently unavailable");
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Void>> handleUnexpected(Exception e) {
        log.error("Unhandled exception serving a support request", e);
        return respond(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR",
                "An unexpected error occurred");
    }

    private ResponseEntity<ApiResponse<Void>> respond(HttpStatus status, String code, String message) {
        return ResponseEntity.status(status).body(ApiResponse.failure(ApiError.of(code, message)));
    }
}
