package com.apargo.services.webhook;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * webhook-service.
 *
 * <p>Owns the single Meta callback URL for the platform. Its entire job is to verify the signature,
 * reject duplicates, persist durably, return 200, and publish to Kafka. No business logic, no tenant
 * resolution, no calls to other services — because exactly one codebase is responsible for never
 * losing a Meta event, and it must have nothing in it that could break that.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class WebhookApplication {

    public static void main(String[] args) {
        SpringApplication.run(WebhookApplication.class, args);
    }
}
