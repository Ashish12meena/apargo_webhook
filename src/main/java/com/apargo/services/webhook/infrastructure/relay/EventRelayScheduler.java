package com.apargo.services.webhook.infrastructure.relay;

import com.apargo.services.webhook.application.service.EventRelayService;
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

    private final EventRelayService relayService;

    public EventRelayScheduler(EventRelayService relayService) {
        this.relayService = relayService;
    }

    @Scheduled(
            fixedDelayString = "${webhook.relay.poll-interval:500ms}",
            initialDelayString = "${webhook.relay.poll-interval:500ms}")
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
