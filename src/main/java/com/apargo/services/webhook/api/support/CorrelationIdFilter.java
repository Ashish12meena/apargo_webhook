package com.apargo.services.webhook.api.support;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import org.slf4j.MDC;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Puts a correlation id on every request, in the response header and in the logging context.
 *
 * <p>Accepts an inbound id so a trace started by a gateway or another service survives the hop.
 * Meta will not send one, so most webhook requests get a fresh id — which is still what joins the
 * ingest log line to the relay log line for the same event.
 */
public class CorrelationIdFilter extends OncePerRequestFilter {

    public static final String CORRELATION_ID_KEY = "correlationId";
    public static final String HEADER = "X-Correlation-Id";

    @Override
    protected void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {

        String correlationId = request.getHeader(HEADER);
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }

        MDC.put(CORRELATION_ID_KEY, correlationId);
        response.setHeader(HEADER, correlationId);
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(CORRELATION_ID_KEY);
        }
    }
}
