package com.apargo.services.webhook.api.v1.dto;

import com.apargo.services.webhook.domain.model.EventState;
import com.apargo.services.webhook.domain.model.Lane;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;

/**
 * Bulk replay filter.
 *
 * <p>{@code from} and {@code to} are mandatory at the type level as well as being checked in the
 * service. An unbounded replay of thirty days into the status topic is an incident, and it must not
 * be reachable by forgetting a parameter.
 */
public record BulkReplayRequest(
        @NotNull(message = "from is required; bulk replay must be bounded") Instant from,
        @NotNull(message = "to is required; bulk replay must be bounded") Instant to,
        EventState state,
        Lane lane,
        String field,
        String providerPhoneNumberId) {
}
