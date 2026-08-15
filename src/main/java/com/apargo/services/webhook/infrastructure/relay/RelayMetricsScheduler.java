package com.apargo.services.webhook.infrastructure.relay;

import com.apargo.services.webhook.application.port.out.WebhookEventRepositoryPort;
import com.apargo.services.webhook.domain.model.EventState;
import com.apargo.services.webhook.infrastructure.metrics.WebhookMetrics;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Refreshes the relay health gauges.
 *
 * <p>The number to alert on is the lag — the age of the oldest pending event — rather than the
 * pending count. Depth spikes during a campaign burst are normal and drain on their own; a document
 * sitting unpublished for five minutes means the relay is stuck and nobody downstream knows it.
 */
@Slf4j
@Component
public class RelayMetricsScheduler {

    private final WebhookEventRepositoryPort repository;
    private final WebhookMetrics metrics;
    private final Clock clock;

    public RelayMetricsScheduler(
            WebhookEventRepositoryPort repository, WebhookMetrics metrics, Clock clock) {
        this.repository = repository;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Scheduled(
            fixedDelayString = "${webhook.relay.metrics-interval:30s}",
            initialDelayString = "${webhook.relay.metrics-interval:30s}")
    public void refresh() {
        try {
            long pending = repository.countByState(EventState.PENDING);
            long failed = repository.countByState(EventState.FAILED);
            long lagSeconds = repository.oldestPendingReceivedAt()
                    .map(oldest -> Math.max(0, Duration.between(oldest, Instant.now(clock)).toSeconds()))
                    .orElse(0L);

            metrics.updateRelayGauges(pending, failed, lagSeconds);
        } catch (Exception e) {
            log.warn("Could not refresh relay gauges: {}", e.getMessage());
        }
    }
}
