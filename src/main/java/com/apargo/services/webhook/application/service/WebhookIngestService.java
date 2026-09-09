package com.apargo.services.webhook.application.service;

import com.apargo.services.webhook.application.port.in.IngestWebhookUseCase;
import com.apargo.services.webhook.application.port.out.DedupePort;
import com.apargo.services.webhook.application.port.out.MetaVerifierPort;
import com.apargo.services.webhook.domain.exception.InvalidSignatureException;
import com.apargo.services.webhook.domain.exception.UnparseablePayloadException;
import com.apargo.services.webhook.domain.model.IngestResult;
import com.apargo.services.webhook.domain.model.WebhookEvent;
import com.apargo.services.webhook.infrastructure.config.WebhookProperties;
import com.apargo.services.webhook.infrastructure.metrics.MetricNames;
import com.apargo.services.webhook.infrastructure.metrics.WebhookMetrics;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * The ingest path, in the only order that is correct:
 * verify, dedupe, split, persist, and only then publish.
 *
 * <p>The durable write happens before Meta is told 200. Acknowledging first and persisting after
 * means an unclean shutdown silently loses events that can never be recovered — Meta offers no event
 * log, no replay API and no dead-letter queue.
 *
 * <p>The batch is then handed straight to Kafka from memory. It is already there when Mongo
 * acknowledges the insert; the previous design wrote it, and then had the relay claim the same
 * documents back out of the database one round trip at a time to recover data the process had never
 * let go of. The sequence is strict — Mongo acknowledges, then Kafka — because publishing first and
 * failing the write afterwards puts events on the topic with no record of them.
 *
 * <p>Nothing else happens on this path. No tenant resolution, no enrichment, no calls to other
 * services — the moment this service depends on another one, that service is on the critical path
 * for every webhook on the platform.
 */
@Slf4j
@Service
public class WebhookIngestService implements IngestWebhookUseCase {

    /** The only value Meta sends for {@code hub.mode}. Anything else is not Meta. */
    private static final String SUBSCRIBE_MODE = "subscribe";

    private final MetaVerifierPort verifier;
    private final DedupePort dedupe;
    private final BodyHasher bodyHasher;
    private final WebhookSplitter splitter;
    private final IngestWriter writer;
    private final FastPathPublisher fastPath;
    private final WebhookMetrics metrics;
    private final WebhookProperties properties;
    private final Clock clock;

    public WebhookIngestService(
            MetaVerifierPort verifier,
            DedupePort dedupe,
            BodyHasher bodyHasher,
            WebhookSplitter splitter,
            IngestWriter writer,
            FastPathPublisher fastPath,
            WebhookMetrics metrics,
            WebhookProperties properties,
            Clock clock) {
        this.verifier = verifier;
        this.dedupe = dedupe;
        this.bodyHasher = bodyHasher;
        this.splitter = splitter;
        this.writer = writer;
        this.fastPath = fastPath;
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
            metrics.recordRejected(MetricNames.REASON_SIGNATURE);
            throw e;
        }
    }

    private IngestResult storeChanges(byte[] rawBody, String bodyHash, Instant receivedAt) {
        List<WebhookEvent> events;
        try {
            events = splitter.split(rawBody, bodyHash, receivedAt);
        } catch (UnparseablePayloadException e) {
            metrics.recordRejected(MetricNames.REASON_UNPARSEABLE);
            throw e;
        }

        if (events.isEmpty()) {
            log.info("Webhook body carried no changes, hash={}", bodyHash);
            return IngestResult.empty();
        }

        List<WebhookEvent> stored = persist(events);
        fastPath.handOff(stored);

        logStored(stored);
        return IngestResult.stored(stored.size());
    }

    /**
     * Writes the batch durably, with each record hidden from the recovery worker for one grace
     * window.
     *
     * <p>This is the subtlest part of the design. Every record is PENDING for the few hundred
     * milliseconds between its insert and its PUBLISHED mark, while the in-memory batch is already
     * in flight to Kafka. A recovery worker scanning for {@code state = PENDING} with no age filter
     * would find all of them and republish the entire live stream — quietly, since consumers dedupe
     * on wamid and nothing would fail. Setting {@code nextAttemptAt} one grace window ahead means
     * the worker's existing {@code nextAttemptAt <= now} predicate needs no change at all.
     *
     * <p>The grace applies only when the fast path is on. With it off there is nothing in flight to
     * protect, and a grace would be pure added latency before the recovery worker could pick the
     * record up.
     */
    private List<WebhookEvent> persist(List<WebhookEvent> events) {
        Duration grace = fastPath.isEnabled() ? properties.relay().grace() : Duration.ZERO;

        List<WebhookEvent> toStore;
        if (grace.isZero()) {
            toStore = events;
        } else {
            toStore = new ArrayList<>(events.size());
            for (WebhookEvent event : events) {
                Instant visibleAt = event.receivedAt() == null
                        ? Instant.now(clock).plus(grace)
                        : event.receivedAt().plus(grace);
                toStore.add(event.toBuilder().nextAttemptAt(visibleAt).build());
            }
        }
        return writer.write(toStore);
    }

    /**
     * Meta documents 3 MB as its ceiling and a larger body has never been observed. If one ever
     * arrives, it is stored clipped and flagged rather than dropped: the payload exists nowhere else
     * once Meta has been answered.
     */
    private IngestResult storeTruncated(byte[] rawBody, String bodyHash, Instant receivedAt) {
        int keepBytes = (int) properties.ingest().maxPayloadSize().toBytes();
        WebhookEvent event = splitter.truncatedEvent(rawBody, bodyHash, receivedAt, keepBytes);
        List<WebhookEvent> stored = persist(List.of(event));
        fastPath.handOff(stored);

        log.error("Webhook body of {} bytes exceeded the {} byte ceiling. Stored truncated and "
                        + "flagged as eventId={}. This should never happen — investigate.",
                rawBody.length, keepBytes, stored.isEmpty() ? "unknown" : stored.get(0).id());
        metrics.recordRejected(MetricNames.REASON_OVERSIZED);
        return IngestResult.truncated();
    }

    private boolean isOversized(byte[] rawBody) {
        return rawBody != null && rawBody.length > properties.ingest().maxPayloadSize().toBytes();
    }

    /**
     * One summary line per request, and the per-event detail only at DEBUG.
     *
     * <p>Identifiers only, never the payload: inbound bodies carry customer phone numbers and
     * message text. The volume matters as much as the content — two INFO lines per event appended
     * synchronously to the journal are free at 30 events/sec and become the bottleneck at 2,000.
     */
    private void logStored(List<WebhookEvent> stored) {
        log.info("Stored {} event(s) from one webhook, lanes={}",
                stored.size(), stored.stream().map(WebhookEvent::lane).distinct().toList());

        if (log.isDebugEnabled()) {
            for (WebhookEvent event : stored) {
                log.debug("Stored eventId={} field={} lane={} providerPhoneNumberId={} wamids={} topic={}",
                        event.id(), event.field(), event.lane(),
                        event.providerPhoneNumberId(), event.wamids(), event.topic());
            }
        }
    }
}
