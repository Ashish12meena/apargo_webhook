package com.apargo.services.webhook.infrastructure.config;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

/**
 * Fails startup, loudly and with an actionable message, when configuration is missing or internally
 * inconsistent.
 *
 * <p>Two different kinds of problem are checked here, for the same reason. A missing secret means
 * the service starts successfully and rejects every Meta callback with a 403 — which looks identical
 * to a wrong secret and burns the seven days of Meta retries before anyone notices. A violated
 * invariant is worse: nothing fails at all, and the only symptom is duplicate volume or a stalled
 * gauge weeks later. Both are cheaper to catch in the first second of startup.
 */
@Slf4j
@Component
public class WebhookPropertiesValidator implements InitializingBean {

    private final WebhookProperties properties;

    public WebhookPropertiesValidator(WebhookProperties properties) {
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() {
        List<String> problems = new ArrayList<>();

        checkSecrets(problems);
        checkInvariants(problems);

        if (!problems.isEmpty()) {
            throw new IllegalStateException(
                    "webhook-service configuration is incomplete or inconsistent:"
                            + System.lineSeparator()
                            + String.join(System.lineSeparator(),
                                    problems.stream().map(p -> "  - " + p).toList()));
        }

        warnOnRiskyButLegalSettings();
        logEffectiveTuning();
    }

    private void checkSecrets(List<String> problems) {
        if (isBlank(properties.meta().verifyToken())) {
            problems.add("WHATSAPP_WEBHOOK_VERIFY_TOKEN is not set — the Meta subscription handshake "
                    + "(GET /api/v1/webhook) cannot succeed without it");
        }
        if (properties.meta().verifySignature() && isBlank(properties.meta().appSecret())) {
            problems.add("WHATSAPP_APP_SECRET is not set while webhook.meta.verify-signature is true — "
                    + "every Meta callback would be rejected with 403");
        }
        if (isBlank(properties.internal().apiKey())) {
            problems.add("INTERNAL_API_KEY is not set — the support endpoints under "
                    + "/api/v1/webhook-events would be unprotected");
        }
    }

    private void checkInvariants(List<String> problems) {
        if (!properties.relay().isBackoffRangeOrdered()) {
            problems.add("WEBHOOK_RELAY_BACKOFF_MAX (" + properties.relay().backoffMax()
                    + ") must be greater than or equal to WEBHOOK_RELAY_BACKOFF_BASE ("
                    + properties.relay().backoffBase() + ")");
        }

        // The invariant that duplicates the entire stream when it is wrong, and reports nothing.
        if (!properties.isAckFlushWithinGrace()) {
            problems.add("WEBHOOK_PUBLISHER_ACK_FLUSH_INTERVAL ("
                    + properties.publisher().ackFlushInterval()
                    + ") x " + WebhookDefaults.ACK_FLUSH_INTERVALS_PER_GRACE
                    + " exceeds WEBHOOK_RELAY_GRACE (" + properties.relay().grace()
                    + "). Records would still be PENDING when the recovery worker becomes able to "
                    + "see them, and every event the fast path already delivered would be "
                    + "published a second time. Either lower the flush interval or raise the grace");
        }

        if (properties.ingest().batch().enabled()
                && properties.ingest().batch().maxWait().toMillis()
                        > properties.relay().pollInterval().toMillis()) {
            log.warn("webhook.ingest.batch.max-wait ({}) exceeds the relay poll interval ({}). "
                            + "Every Meta response is delayed by up to the former; see section 5 of "
                            + "the design note before leaving this in place.",
                    properties.ingest().batch().maxWait(), properties.relay().pollInterval());
        }
    }

    private void warnOnRiskyButLegalSettings() {
        if (!properties.meta().verifySignature()) {
            log.warn("SIGNATURE VERIFICATION IS DISABLED. This is acceptable in local development "
                    + "only — any caller can post arbitrary events to this service.");
        }
        if (!properties.relay().enabled()) {
            log.warn("THE RECOVERY WORKER IS DISABLED (WEBHOOK_RELAY_ENABLED=false). Anything the "
                    + "fast path drops — a Kafka outage, a crash between insert and handoff — stays "
                    + "PENDING forever with no error logged anywhere.");
        }
        if (!properties.publisher().fastPathEnabled()) {
            log.warn("The publish fast path is disabled. Every event is routed through the recovery "
                    + "worker instead: correct, but one claim round trip and one poll interval of "
                    + "latency per event.");
        }
    }

    /**
     * Prints what was actually applied. The values that matter here are individually harmless and
     * collectively decide throughput, and reading them from the running service beats inferring them
     * from a chain of .env files, environment variables and defaults.
     */
    private void logEffectiveTuning() {
        log.info("Webhook tuning: fastPath={} maxInFlight={} ackFlush={}/{} | relay batch={} "
                        + "poll={} grace={} lease={} | ingest batching={} | schedulerPool={}",
                properties.publisher().fastPathEnabled(),
                properties.publisher().maxInFlight(),
                properties.publisher().ackFlushSize(),
                properties.publisher().ackFlushInterval(),
                properties.relay().batchSize(),
                properties.relay().pollInterval(),
                properties.relay().grace(),
                properties.relay().lease(),
                properties.ingest().batch().enabled(),
                properties.scheduling().poolSize());
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
