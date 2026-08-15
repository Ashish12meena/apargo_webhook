package com.apargo.services.webhook.infrastructure.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Scheduling is enabled in its own class rather than on the application class, so that disabling or
 * replacing it is a one-file change and the application class stays free of infrastructure concerns.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
public class SchedulingConfig {
}
