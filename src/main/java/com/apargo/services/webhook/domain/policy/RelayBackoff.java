package com.apargo.services.webhook.domain.policy;

import java.time.Duration;

/**
 * Exponential backoff for relay retries: {@code base} doubling up to {@code max}.
 *
 * <p>Kafka outages are usually minutes long and there is no user waiting on a relay, so the schedule
 * is deliberately patient. Pure domain logic with no Spring dependency, so it is directly unit
 * testable.
 */
public record RelayBackoff(Duration base, Duration max, int maxAttempts) {

    public RelayBackoff {
        if (base == null || base.isNegative() || base.isZero()) {
            throw new IllegalArgumentException("backoff base must be positive");
        }
        if (max == null || max.compareTo(base) < 0) {
            throw new IllegalArgumentException("backoff max must be greater than or equal to base");
        }
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
    }

    /**
     * Delay before the given attempt number is retried.
     *
     * @param attempts the number of attempts already made, starting at 1
     * @return {@code base * 2^(attempts-1)}, capped at {@code max}
     */
    public Duration delayFor(int attempts) {
        int exponent = Math.max(0, attempts - 1);
        if (exponent >= 62) {
            return max;
        }
        long millis = base.toMillis() << exponent;
        // The shift can only overflow beyond the cap, so a negative result is also a cap hit.
        if (millis < 0 || millis > max.toMillis()) {
            return max;
        }
        return Duration.ofMillis(millis);
    }

    /** True when the given attempt count has exhausted the schedule. */
    public boolean isExhausted(int attempts) {
        return attempts >= maxAttempts;
    }
}
