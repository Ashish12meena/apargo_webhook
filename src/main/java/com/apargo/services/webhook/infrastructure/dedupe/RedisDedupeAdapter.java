package com.apargo.services.webhook.infrastructure.dedupe;

import com.apargo.services.webhook.infrastructure.config.WebhookProperties;
import com.apargo.services.webhook.infrastructure.persistence.PersistenceConstants;
import java.time.Duration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * The preferred dedupe store: a single {@code SETNX} with a TTL, sub-millisecond and safe to lose.
 *
 * <p>Redis is used for dedupe and nothing else. It never buffers payloads. The asymmetry is the
 * whole argument: dedupe state is reconstructible, whereas a webhook payload exists nowhere else in
 * the world once Meta has been told 200.
 */
@Component
public class RedisDedupeAdapter implements DedupeStore {

    private final StringRedisTemplate redisTemplate;
    private final Duration ttl;

    public RedisDedupeAdapter(StringRedisTemplate redisTemplate, WebhookProperties properties) {
        this.redisTemplate = redisTemplate;
        this.ttl = properties.ingest().dedupeTtl();
    }

    @Override
    public boolean markSeen(String bodyHash) {
        Boolean firstTime = redisTemplate.opsForValue().setIfAbsent(PersistenceConstants.DEDUPE_KEY_PREFIX + bodyHash,
                PersistenceConstants.DEDUPE_MARKER, ttl);
        // A null reply means the command gave no answer; treat the hash as unseen and let the
        // durable store settle it, rather than dropping a possibly-new event.
        return firstTime == null || firstTime;
    }

    @Override
    public String name() {
        return "redis";
    }
}
