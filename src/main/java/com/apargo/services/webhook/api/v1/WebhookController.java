package com.apargo.services.webhook.api.v1;

import com.apargo.services.webhook.application.port.in.IngestWebhookUseCase;
import com.apargo.services.webhook.domain.exception.InvalidSignatureException;
import com.apargo.services.webhook.domain.exception.UnparseablePayloadException;
import com.apargo.services.webhook.domain.model.IngestResult;
import com.apargo.services.webhook.infrastructure.config.WebhookProperties;
import jakarta.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The one callback URL for the whole platform.
 *
 * <p>Status codes here are dictated by Meta's retry behaviour, not by REST convention. Meta retries
 * any non-200 with decreasing frequency for up to seven days and then disables the subscription —
 * for every service, not just this one — and it offers no event log, no replay API and no
 * dead-letter queue. So:
 *
 * <ul>
 *   <li>403 only when the signature fails, because that request is not from Meta.
 *   <li>400 only when the body cannot be parsed, because no retry will fix it.
 *   <li>500 only when the store is unavailable, because that is the one case where a Meta retry is
 *       genuinely what we want.
 *   <li>200 for everything else, including unexpected exceptions.
 * </ul>
 *
 * <p>Exceptions are handled here rather than by the global advice precisely so that no future
 * handler can turn one of these into a 5xx by accident.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/webhook")
public class WebhookController {

    private final IngestWebhookUseCase ingestUseCase;
    private final WebhookProperties properties;

    public WebhookController(IngestWebhookUseCase ingestUseCase, WebhookProperties properties) {
        this.ingestUseCase = ingestUseCase;
        this.properties = properties;
    }

    /**
     * Subscription handshake. Meta calls this once, when the URL is configured in the App Dashboard.
     * The challenge must come back as plain text, unwrapped.
     */
    @GetMapping(produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> verifySubscription(
            @RequestParam(name = "hub.mode", required = false) String mode,
            @RequestParam(name = "hub.verify_token", required = false) String verifyToken,
            @RequestParam(name = "hub.challenge", required = false) String challenge) {

        return ingestUseCase.resolveHandshake(mode, verifyToken, challenge)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.FORBIDDEN).build());
    }

    /**
     * Event ingest.
     *
     * <p>The body is taken as {@code byte[]} rather than a mapped DTO because the HMAC is computed
     * over the raw bytes. Verifying a re-serialised body can never match: Jackson reorders keys and
     * normalises whitespace.
     */
    @PostMapping(consumes = MediaType.ALL_VALUE)
    public ResponseEntity<Void> ingest(
            @RequestBody(required = false) byte[] rawBody, HttpServletRequest request) {

        String signature = request.getHeader(properties.meta().signatureHeader());

        try {
            IngestResult result = ingestUseCase.ingest(rawBody, signature);
            log.debug("Ingest outcome={} stored={}", result.outcome(), result.storedCount());
            return ResponseEntity.ok().build();

        } catch (InvalidSignatureException e) {
            log.warn("Rejected webhook from {}: {}", request.getRemoteAddr(), e.getMessage());
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();

        } catch (UnparseablePayloadException e) {
            // The one place a body is logged in full. It cannot be parsed, so it is not a message we
            // can extract identifiers from, and without it the failure cannot be diagnosed at all.
            log.error("Unparseable webhook body from {}: {} | body={}",
                    request.getRemoteAddr(), e.getMessage(), asText(rawBody));
            return ResponseEntity.badRequest().build();

        } catch (DataAccessException e) {
            // The only case that should 500. Answering 200 while unable to persist is the one thing
            // that loses a Meta event irrecoverably; here the retry is exactly what we want.
            log.error("Event store unavailable; answering 500 so Meta retries", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();

        } catch (Exception e) {
            // A 5xx would start Meta's retry cycle for something a retry cannot fix.
            log.error("Unexpected failure ingesting a webhook; answering 200 to protect the "
                    + "subscription. This event may have been lost — investigate.", e);
            return ResponseEntity.ok().build();
        }
    }

    private String asText(byte[] rawBody) {
        return rawBody == null ? "<empty>" : new String(rawBody, StandardCharsets.UTF_8);
    }
}
