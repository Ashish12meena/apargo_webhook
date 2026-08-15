package com.apargo.services.webhook.infrastructure.config;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

/**
 * Fails startup, loudly and with an actionable message, when a required secret is missing.
 *
 * <p>The alternative — starting successfully and rejecting every Meta callback with a 403 — looks
 * identical to a wrong secret and burns the seven days of Meta retries before anyone notices.
 */
@Slf4j
@Component
public class WebhookPropertiesValidator implements InitializingBean {

    private final WebhookProperties properties;

    public WebhookPropertiesValidator(WebhookProperties properties) {
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() {
        List<String> problems = new ArrayList<>();

        if (isBlank(properties.meta().verifyToken())) {
            problems.add("WHATSAPP_WEBHOOK_VERIFY_TOKEN is not set — the Meta subscription handshake "
                    + "(GET /api/v1/webhook) cannot succeed without it");
        }
        if (properties.meta().verifySignature() && isBlank(properties.meta().appSecret())) {
            problems.add("WHATSAPP_APP_SECRET is not set while webhook.meta.verify-signature is true — "
                    + "every Meta callback would be rejected with 403");
        }
        if (isBlank(properties.internal().apiKey())) {
            problems.add("INTERNAL_API_KEY is not set — the support endpoints under "
                    + "/api/v1/webhook-events would be unprotected");
        }
        if (properties.relay().backoffMax().compareTo(properties.relay().backoffBase()) < 0) {
            problems.add("webhook.relay.backoff-max must be greater than or equal to backoff-base");
        }

        if (!problems.isEmpty()) {
            throw new IllegalStateException(
                    "webhook-service configuration is incomplete. Populate these values in .env:"
                            + System.lineSeparator()
                            + String.join(System.lineSeparator(), problems.stream().map(p -> "  - " + p).toList()));
        }

        if (!properties.meta().verifySignature()) {
            log.warn("SIGNATURE VERIFICATION IS DISABLED. This is acceptable in local development "
                    + "only — any caller can post arbitrary events to this service.");
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
