package com.apargo.services.webhook.domain.model;

import java.time.Instant;
import lombok.Builder;

/**
 * Filter for the support read and bulk replay paths.
 *
 * @param state                 optional relay state filter
 * @param lane                  optional lane filter
 * @param field                 optional Meta field filter
 * @param providerPhoneNumberId optional business phone number filter
 * @param wamid                 optional wamid contained in the change
 * @param from                  inclusive lower bound on receivedAt
 * @param to                    exclusive upper bound on receivedAt
 * @param page                  zero-based page index
 * @param size                  page size
 */
@Builder
public record EventSearchCriteria(
        EventState state,
        Lane lane,
        String field,
        String providerPhoneNumberId,
        String wamid,
        Instant from,
        Instant to,
        int page,
        int size) {

    /** True when both bounds are present — required before any bulk replay is allowed. */
    public boolean hasBoundedRange() {
        return from != null && to != null;
    }
}
