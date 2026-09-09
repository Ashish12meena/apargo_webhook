package com.apargo.services.webhook.infrastructure.relay;

import com.apargo.services.webhook.application.service.EventRelayService;
import com.apargo.services.webhook.infrastructure.config.WebhookDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives the outbox drain.
 *
 * <p>The trigger is separated from {@link EventRelayService} so the relay logic can be unit tested
 * without a scheduler, and so an instance can run with the relay disabled — useful when scaling the
 * ingest tier independently of the publishing tier.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "webhook.relay", name = "enabled", havingValue = "true", matchIfMissing = true)
public class EventRelayScheduler {

    /**
     * The placeholder and its fallback, in one constant so the two cannot disagree. The fallback is
     * {@link WebhookDefaults} rather than a literal, so there is exactly one place in the codebase
     * that says what the poll interval is when nobody configures it.
     */
    private static final String POLL_INTERVAL =
            "${webhook.relay.poll-interval:" + WebhookDefaults.RELAY_POLL_INTERVAL + "}";

    private final EventRelayService relayService;

    public EventRelayScheduler(EventRelayService relayService) {
        this.relayService = relayService;
    }

    @Scheduled(
            fixedDelayString = POLL_INTERVAL,
            initialDelayString = POLL_INTERVAL)
    public void drain() {
        try {
            relayService.drainOnce();
        } catch (Exception e) {
            // Never let a poll failure kill the schedule; the documents are durable and the next
            // tick will pick them up.
            log.error("Relay poll failed; will retry on the next tick", e);
        }
    }
}
