package com.apargo.services.webhook.infrastructure.messaging;

/**
 * The Kafka record headers this service sets.
 *
 * <p>They are part of the published contract: consumers filter and log on them without deserialising
 * the body, so renaming one is a breaking change for every service on the platform. They live here
 * rather than inline in the publisher so that is obvious.
 */
public final class MessagingConstants {

    /** The Mongo {@code _id}. The correlation key across every service and the replay endpoint. */
    public static final String HEADER_EVENT_ID = "eventId";

    public static final String HEADER_LANE = "lane";

    public static final String HEADER_FIELD = "field";

    private MessagingConstants() {
    }
}
