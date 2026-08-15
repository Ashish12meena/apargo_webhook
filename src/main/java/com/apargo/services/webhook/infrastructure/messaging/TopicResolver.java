package com.apargo.services.webhook.infrastructure.messaging;

import com.apargo.services.webhook.application.port.out.TopicResolverPort;
import com.apargo.services.webhook.domain.model.Lane;
import com.apargo.services.webhook.infrastructure.config.WebhookProperties;
import org.springframework.stereotype.Component;

/**
 * Lane to topic, resolved once at split time and denormalised onto the document so the relay does no
 * lookup and a topic rename cannot strand documents already in flight.
 */
@Component
public class TopicResolver implements TopicResolverPort {

    private final WebhookProperties.Topics topics;

    public TopicResolver(WebhookProperties properties) {
        this.topics = properties.topics();
    }

    @Override
    public String resolve(Lane lane) {
        return switch (lane) {
            case INBOUND -> topics.inbound();
            case STATUS -> topics.status();
            case TEMPLATE -> topics.template();
            case ACCOUNT -> topics.account();
            case USER_PREFERENCE -> topics.userPreference();
            case OTHER -> topics.unrouted();
        };
    }
}
