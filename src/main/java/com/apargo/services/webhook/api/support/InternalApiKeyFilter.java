package com.apargo.services.webhook.api.support;

import com.apargo.services.webhook.infrastructure.config.WebhookProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Guards the internal support plane with a shared key, exactly as the other services on the platform.
 *
 * <p>Registered against the support paths only. The Meta-facing endpoints are authenticated by the
 * HMAC signature instead — Meta cannot send a private header.
 */
@Slf4j
public class InternalApiKeyFilter extends OncePerRequestFilter {

    private final WebhookProperties properties;
    private final ObjectMapper objectMapper;

    public InternalApiKeyFilter(WebhookProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {

        String presented = request.getHeader(properties.internal().headerName());
        if (!matches(presented, properties.internal().apiKey())) {
            log.warn("Rejected internal request to {} from {}: missing or invalid API key",
                    request.getRequestURI(), request.getRemoteAddr());
            writeUnauthorized(response);
            return;
        }
        chain.doFilter(request, response);
    }

    private void writeUnauthorized(HttpServletResponse response) throws IOException {
        response.setStatus(HttpStatus.UNAUTHORIZED.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        objectMapper.writeValue(
                response.getOutputStream(),
                ApiResponse.failure(ApiError.of("UNAUTHORIZED",
                        "A valid " + properties.internal().headerName() + " header is required")));
    }

    /** Constant-time comparison through a digest, so neither length nor prefix leaks via timing. */
    private boolean matches(String presented, String expected) {
        if (presented == null || expected == null || expected.isBlank()) {
            return false;
        }
        return MessageDigest.isEqual(digest(presented), digest(expected));
    }

    private byte[] digest(String value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
