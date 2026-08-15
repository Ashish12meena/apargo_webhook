package com.apargo.services.webhook.infrastructure.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Topic definitions.
 *
 * <p>Inbound and status are deliberately separate topics. Inbound is low volume and latency
 * critical — a customer's reply arriving three seconds late is visible to an agent. Status is high
 * volume and latency tolerant. Sharing a topic would put a campaign burst of 150k statuses in front
 * of one customer's reply; separate topics with separate consumer groups make that impossible.
 *
 * <p>Partition counts follow from the keys: status is keyed by wamid and carries the most traffic,
 * inbound is keyed by conversation, and the unrouted dead letter needs exactly one.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "webhook.topics", name = "auto-create", havingValue = "true", matchIfMissing = true)
public class KafkaConfig {

    @Bean
    public KafkaAdmin.NewTopics webhookTopics(WebhookProperties properties) {
        WebhookProperties.Topics topics = properties.topics();
        WebhookProperties.Topics.Partitions partitions = topics.partitions();
        int replicas = topics.replicas();

        return new KafkaAdmin.NewTopics(
                topic(topics.inbound(), partitions.inbound(), replicas),
                topic(topics.status(), partitions.status(), replicas),
                topic(topics.template(), partitions.template(), replicas),
                topic(topics.account(), partitions.account(), replicas),
                topic(topics.userPreference(), partitions.userPreference(), replicas),
                topic(topics.unrouted(), partitions.unrouted(), replicas));
    }

    private NewTopic topic(String name, int partitions, int replicas) {
        return TopicBuilder.name(name).partitions(partitions).replicas(replicas).build();
    }
}
