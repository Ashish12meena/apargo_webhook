package com.apargo.services.webhook.api.support;

import com.apargo.services.webhook.infrastructure.config.WebhookProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

/**
 * Filter registration in one place, with explicit ordering and explicit URL patterns.
 *
 * <p>The filters are plain classes rather than {@code @Component}s so that Boot cannot also register
 * them against every path by component scanning, which would silently put the internal API key check
 * in front of Meta's callbacks.
 */
@Configuration(proxyBeanMethods = false)
public class WebFilterConfig {

    /** Support endpoints guarded by the internal key. Keep in sync with WebhookEventController. */
    private static final String INTERNAL_PATH_PATTERN = "/api/v1/webhook-events/*";
    private static final String INTERNAL_ROOT_PATTERN = "/api/v1/webhook-events";

    @Bean
    public FilterRegistrationBean<CorrelationIdFilter> correlationIdFilter() {
        FilterRegistrationBean<CorrelationIdFilter> registration =
                new FilterRegistrationBean<>(new CorrelationIdFilter());
        registration.addUrlPatterns("/*");
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return registration;
    }

    @Bean
    public FilterRegistrationBean<InternalApiKeyFilter> internalApiKeyFilter(
            WebhookProperties properties, ObjectMapper objectMapper) {
        FilterRegistrationBean<InternalApiKeyFilter> registration =
                new FilterRegistrationBean<>(new InternalApiKeyFilter(properties, objectMapper));
        registration.addUrlPatterns(INTERNAL_ROOT_PATTERN, INTERNAL_PATH_PATTERN);
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE + 10);
        return registration;
    }
}
