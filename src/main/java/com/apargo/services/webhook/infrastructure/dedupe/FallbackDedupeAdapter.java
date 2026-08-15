package com.apargo.services.webhook.infrastructure.dedupe;

import com.apargo.services.webhook.application.port.out.DedupePort;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Tries Redis, falls back to Mongo, and never fails the request either way.
 *
 * <p>Dedupe is an optimisation. A duplicate that slips through costs one redundant reprocess, which
 * every consumer is required to handle idempotently. A webhook rejected because a dedupe store was
 * unreachable costs a Meta retry — and enough of those disable the subscription for every service on
 * the platform. The asymmetry decides the behaviour: when in doubt, let it through.
 */
@Slf4j
@Component
public class FallbackDedupeAdapter implements DedupePort {

    private final DedupeStore primary;
    private final DedupeStore fallback;

    public FallbackDedupeAdapter(RedisDedupeAdapter primary, MongoDedupeFallback fallback) {
        this.primary = primary;
        this.fallback = fallback;
    }

    @Override
    public boolean markSeen(String bodyHash) {
        try {
            return primary.markSeen(bodyHash);
        } catch (RuntimeException e) {
            log.warn("Dedupe store {} unavailable, falling back to {}: {}",
                    primary.name(), fallback.name(), e.getMessage());
        }

        try {
            return fallback.markSeen(bodyHash);
        } catch (RuntimeException e) {
            log.error("Dedupe store {} also unavailable; processing the event without duplicate "
                    + "suppression. Consumers must dedupe on wamid.", fallback.name(), e);
            return true;
        }
    }
}
