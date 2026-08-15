package com.apargo.services.webhook.application.port.out;

/** Verification of the two Meta secrets. Both comparisons are constant time. */
public interface MetaVerifierPort {

    /**
     * @param rawBody         the exact bytes received
     * @param signatureHeader the {@code X-Hub-Signature-256} value, may be null
     * @throws com.apargo.services.webhook.domain.exception.InvalidSignatureException
     *         when the header is absent, malformed, or does not match
     */
    void verifySignature(byte[] rawBody, String signatureHeader);

    /** @return true when the candidate matches the configured {@code hub.verify_token} */
    boolean matchesVerifyToken(String candidate);
}
