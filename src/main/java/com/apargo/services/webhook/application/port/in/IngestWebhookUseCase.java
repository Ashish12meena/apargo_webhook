package com.apargo.services.webhook.application.port.in;

import com.apargo.services.webhook.domain.model.IngestResult;
import java.util.Optional;

/** The Meta-facing side of the service: subscription handshake and event ingest. */
public interface IngestWebhookUseCase {

    /**
     * Resolves Meta's subscription handshake.
     *
     * @param mode        the {@code hub.mode} parameter
     * @param verifyToken the {@code hub.verify_token} parameter, compared in constant time
     * @param challenge   the {@code hub.challenge} parameter to echo back
     * @return the challenge when the handshake is valid, otherwise empty
     */
    Optional<String> resolveHandshake(String mode, String verifyToken, String challenge);

    /**
     * Verifies, dedupes, splits and durably stores a Meta POST.
     *
     * <p>Returns only after the Mongo write has been acknowledged with {@code w:majority, j:true}.
     * Nothing is published to Kafka on this path.
     *
     * @param rawBody         the exact bytes received; never a re-serialised DTO
     * @param signatureHeader the {@code X-Hub-Signature-256} header value, may be null
     * @throws com.apargo.services.webhook.domain.exception.InvalidSignatureException  signature failed
     * @throws com.apargo.services.webhook.domain.exception.UnparseablePayloadException body is not walkable JSON
     */
    IngestResult ingest(byte[] rawBody, String signatureHeader);
}
