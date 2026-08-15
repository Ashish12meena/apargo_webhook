package com.apargo.services.webhook.application.port.out;

import com.apargo.services.webhook.domain.model.Lane;

/** Resolves a lane to its configured Kafka topic, so the relay never performs a lookup. */
public interface TopicResolverPort {

    String resolve(Lane lane);
}
