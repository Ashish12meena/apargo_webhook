package com.apargo.services.webhook.infrastructure.relay;

import com.apargo.services.webhook.application.service.EventRelayService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Sweeps up after an instance that died holding claims.
 *
 * <p>Without this, every ungraceful shutdown would strand its in-flight batch in {@code PUBLISHING}
 * forever — stored, acknowledged to Meta, and never delivered to anyone.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "webhook.relay", name = "enabled", havingValue = "true", matchIfMissing = true)
public class LeaseReclaimScheduler {

    private final EventRelayService relayService;

    public LeaseReclaimScheduler(EventRelayService relayService) {
        this.relayService = relayService;
    }

    @Scheduled(
            fixedDelayString = "${webhook.relay.lease-reclaim-interval:30s}",
            initialDelayString = "${webhook.relay.lease-reclaim-interval:30s}")
    public void reclaim() {
        try {
            relayService.reclaimExpiredLeases();
        } catch (Exception e) {
            log.error("Lease reclaim sweep failed; will retry on the next tick", e);
        }
    }
}
