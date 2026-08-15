package com.apargo.services.webhook.application.service;

import com.apargo.services.webhook.application.port.in.IngestWebhookUseCase;
import com.apargo.services.webhook.application.port.out.DedupePort;
import com.apargo.services.webhook.application.port.out.MetaVerifierPort;
import com.apargo.services.webhook.application.port.out.WebhookEventRepositoryPort;
import com.apargo.services.webhook.domain.exception.InvalidSignatureException;
import com.apargo.services.webhook.domain.exception.UnparseablePayloadException;
import com.apargo.services.webhook.domain.model.IngestResult;
import com.apargo.services.webhook.domain.model.WebhookEvent;
import com.apargo.services.webhook.infrastructure.config.WebhookProperties;
import com.apargo.services.webhook.infrastructure.metrics.WebhookMetrics;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * The ingest path, in the only order that is correct:
 * verify, dedupe, split, persist, and only then answer.
 *
 * <p>The 200 is returned after the Mongo write and before any Kafka publish. Publishing inline would
 * make a broker blip into a Meta unsubscribe; acknowledging before persisting would silently lose
 * events on an unclean shutdown that Meta will never send again.
 *
 * <p>Nothing else happens on this path. No tenant resolution, no enrichment, no calls to other
 * services — the moment this service depends on another one, that service is on the critical path
 * for every webhook on the platform.
 */
@Slf4j
@Service
public class WebhookIngestService implements IngestWebhookUseCase {

    private static final String SUBSCRIBE_MODE = "subscribe";

    private final MetaVerifierPort verifier;
    private final DedupePort dedupe;
    private final BodyHasher bodyHasher;
    private final WebhookSplitter splitter;
    private final WebhookEventRepositoryPort repository;
    private final WebhookMetrics metrics;
    private final WebhookProperties properties;
    private final Clock clock;

    public WebhookIngestService(
            MetaVerifierPort verifier,
            DedupePort dedupe,
            BodyHasher bodyHasher,
            WebhookSplitter splitter,
            WebhookEventRepositoryPort repository,
            WebhookMetrics metrics,
            WebhookProperties properties,
            Clock clock) {
        this.verifier = verifier;
        this.dedupe = dedupe;
        this.bodyHasher = bodyHasher;
        this.splitter = splitter;
        this.repository = repository;
        this.metrics = metrics;
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public Optional<String> resolveHandshake(String mode, String verifyToken, String challenge) {
        if (!SUBSCRIBE_MODE.equals(mode)) {
            log.warn("Rejected subscription handshake: unexpected hub.mode");
            return Optional.empty();
        }
        if (!verifier.matchesVerifyToken(verifyToken)) {
            log.warn("Rejected subscription handshake: verify token did not match");
            return Optional.empty();
        }
        log.info("Subscription handshake accepted");
        return Optional.ofNullable(challenge);
    }

    @Override
    public IngestResult ingest(byte[] rawBody, String signatureHeader) {
        Timer.Sample sample = metrics.startIngestTimer();
        metrics.recordReceived();
        try {
            verifySignature(rawBody, signatureHeader);

            String bodyHash = bodyHasher.hash(rawBody);
            if (!dedupe.markSeen(bodyHash)) {
                metrics.recordDuplicate();
                log.debug("Duplicate webhook body suppressed, hash={}", bodyHash);
                return IngestResult.duplicate();
            }

            Instant receivedAt = Instant.now(clock);
            if (isOversized(rawBody)) {
                return storeTruncated(rawBody, bodyHash, receivedAt);
            }
            return storeChanges(rawBody, bodyHash, receivedAt);
        } finally {
            metrics.stopIngestTimer(sample);
        }
    }

    private void verifySignature(byte[] rawBody, String signatureHeader) {
        try {
            verifier.verifySignature(rawBody, signatureHeader);
        } catch (InvalidSignatureException e) {
            metrics.recordRejected("signature");
            throw e;
        }
    }

    private IngestResult storeChanges(byte[] rawBody, String bodyHash, Instant receivedAt) {
        List<WebhookEvent> events;
        try {
            events = splitter.split(rawBody, bodyHash, receivedAt);
        } catch (UnparseablePayloadException e) {
            metrics.recordRejected("unparseable");
            throw e;
        }

        if (events.isEmpty()) {
            log.info("Webhook body carried no changes, hash={}", bodyHash);
            return IngestResult.empty();
        }

        List<WebhookEvent> stored = repository.insertAll(events);
        stored.forEach(this::logStored);
        return IngestResult.stored(stored.size());
    }

    /**
     * Meta documents 3 MB as its ceiling and a larger body has never been observed. If one ever
     * arrives, it is stored clipped and flagged rather than dropped: the payload exists nowhere else
     * once Meta has been answered.
     */
    private IngestResult storeTruncated(byte[] rawBody, String bodyHash, Instant receivedAt) {
        int keepBytes = (int) properties.ingest().maxPayloadSize().toBytes();
        WebhookEvent event = splitter.truncatedEvent(rawBody, bodyHash, receivedAt, keepBytes);
        List<WebhookEvent> stored = repository.insertAll(List.of(event));

        log.error("Webhook body of {} bytes exceeded the {} byte ceiling. Stored truncated and "
                        + "flagged as eventId={}. This should never happen — investigate.",
                rawBody.length, keepBytes, stored.isEmpty() ? "unknown" : stored.get(0).id());
        metrics.recordRejected("oversized");
        return IngestResult.truncated();
    }

    private boolean isOversized(byte[] rawBody) {
        return rawBody != null && rawBody.length > properties.ingest().maxPayloadSize().toBytes();
    }

    /**
     * Logs identifiers only. Never the payload: inbound bodies carry customer phone numbers and
     * message text.
     */
    private void logStored(WebhookEvent event) {
        log.info("Stored eventId={} field={} lane={} providerPhoneNumberId={} wamids={} topic={}",
                event.id(), event.field(), event.lane(),
                event.providerPhoneNumberId(), event.wamids(), event.topic());
    }
}
