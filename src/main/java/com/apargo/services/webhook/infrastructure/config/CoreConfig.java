package com.apargo.services.webhook.infrastructure.config;

import com.apargo.services.webhook.domain.policy.RelayBackoff;
import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Shared beans that have no better home, kept together rather than scattered across the codebase. */
@Configuration(proxyBeanMethods = false)
public class CoreConfig {

    /**
     * A single injected clock, so every timestamp in the service comes from one place and every time
     * dependent behaviour is testable without sleeping.
     */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public RelayBackoff relayBackoff(WebhookProperties properties) {
        WebhookProperties.Relay relay = properties.relay();
        return new RelayBackoff(relay.backoffBase(), relay.backoffMax(), relay.maxAttempts());
    }
}
