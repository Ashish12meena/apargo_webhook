package com.apargo.services.webhook.infrastructure.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

/**
 * Every tunable this service has, in one place, bound from {@code application.yml} which in turn
 * reads only environment variables. Nothing else in the codebase reads configuration directly.
 */
@Validated
@ConfigurationProperties(prefix = "webhook")
public record WebhookProperties(
        @Valid @NotNull Meta meta,
        @Valid @NotNull Ingest ingest,
        @Valid @NotNull Relay relay,
        @Valid @NotNull Topics topics,
        @Valid @NotNull Retention retention,
        @Valid @NotNull Internal internal) {

    /**
     * Meta credentials. Neither secret has a default: a secret with a fallback value is a secret in
     * git history. They are validated at startup by {@link WebhookPropertiesValidator}.
     *
     * @param verifySignature set false ONLY in local development, never in a deployed environment
     */
    public record Meta(
            @DefaultValue("") String verifyToken,
            @DefaultValue("") String appSecret,
            @DefaultValue("X-Hub-Signature-256") @NotBlank String signatureHeader,
            @DefaultValue("true") boolean verifySignature) {
    }

    /**
     * @param maxPayloadSize Meta's documented ceiling; a larger body is stored truncated, not dropped
     * @param dedupeTtl      how long a body hash is remembered
     */
    public record Ingest(
            @DefaultValue("3MB") @NotNull DataSize maxPayloadSize,
            @DefaultValue("24h") @NotNull Duration dedupeTtl) {
    }

    /**
     * Outbox relay tuning. Kafka outages are usually minutes and no user is waiting, so the schedule
     * is deliberately patient.
     */
    public record Relay(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("500ms") @NotNull Duration pollInterval,
            @DefaultValue("100") @Min(1) int batchSize,
            @DefaultValue("30s") @NotNull Duration lease,
            @DefaultValue("30s") @NotNull Duration leaseReclaimInterval,
            @DefaultValue("10") @Min(1) int maxAttempts,
            @DefaultValue("1s") @NotNull Duration backoffBase,
            @DefaultValue("5m") @NotNull Duration backoffMax,
            @DefaultValue("30s") @NotNull Duration metricsInterval) {
    }

    /** Topic names and their partition counts. One topic per lane; consumers filter. */
    public record Topics(
            @DefaultValue("whatsapp.webhook.inbound") @NotBlank String inbound,
            @DefaultValue("whatsapp.webhook.status") @NotBlank String status,
            @DefaultValue("whatsapp.webhook.template") @NotBlank String template,
            @DefaultValue("whatsapp.webhook.account") @NotBlank String account,
            @DefaultValue("whatsapp.webhook.user-preference") @NotBlank String userPreference,
            @DefaultValue("whatsapp.webhook.unrouted") @NotBlank String unrouted,
            @DefaultValue("true") boolean autoCreate,
            @DefaultValue("1") @Min(1) int replicas,
            @Valid @NotNull Partitions partitions) {

        public record Partitions(
                @DefaultValue("6") @Min(1) int inbound,
                @DefaultValue("12") @Min(1) int status,
                @DefaultValue("3") @Min(1) int template,
                @DefaultValue("3") @Min(1) int account,
                @DefaultValue("3") @Min(1) int userPreference,
                @DefaultValue("1") @Min(1) int unrouted) {
        }
    }

    /** @param eventTtl drives the TTL index on {@code webhook_events.receivedAt} */
    public record Retention(@DefaultValue("30d") @NotNull Duration eventTtl) {
    }

    /** Internal support plane, guarded exactly as the other services on the platform. */
    public record Internal(
            @DefaultValue("") String apiKey,
            @DefaultValue("X-Internal-Api-Key") @NotBlank String headerName) {
    }
}
