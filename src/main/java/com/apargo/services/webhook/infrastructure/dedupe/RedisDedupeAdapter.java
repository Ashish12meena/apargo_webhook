package com.apargo.services.webhook.infrastructure.dedupe;

import com.apargo.services.webhook.infrastructure.config.WebhookProperties;
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

    private static final String KEY_PREFIX = "wh:dedupe:";
    private static final String MARKER = "1";

    private final StringRedisTemplate redisTemplate;
    private final Duration ttl;

    public RedisDedupeAdapter(StringRedisTemplate redisTemplate, WebhookProperties properties) {
        this.redisTemplate = redisTemplate;
        this.ttl = properties.ingest().dedupeTtl();
    }

    @Override
    public boolean markSeen(String bodyHash) {
        Boolean firstTime = redisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + bodyHash, MARKER, ttl);
        // A null reply means the command gave no answer; treat the hash as unseen and let the
        // durable store settle it, rather than dropping a possibly-new event.
        return firstTime == null || firstTime;
    }

    @Override
    public String name() {
        return "redis";
    }
}
