package com.apargo.services.webhook.infrastructure.security;

import com.apargo.services.webhook.application.port.out.MetaVerifierPort;
import com.apargo.services.webhook.domain.exception.InvalidSignatureException;
import com.apargo.services.webhook.infrastructure.config.WebhookProperties;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Verifies the two Meta secrets.
 *
 * <p>The HMAC is computed over the <em>raw request bytes</em>. Verifying a re-serialised body can
 * never match, because Jackson reorders keys and normalises whitespace — this is the single most
 * common way to get this endpoint wrong.
 *
 * <p>Both comparisons run in constant time. The verify token is compared through a digest so that
 * neither its length nor a shared prefix leaks through timing.
 */
@Slf4j
@Component
public class MetaSignatureVerifier implements MetaVerifierPort {

    private static final String SIGNATURE_PREFIX = "sha256=";
    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final String DIGEST_ALGORITHM = "SHA-256";

    private final WebhookProperties properties;
    private final SecretKeySpec signingKey;

    public MetaSignatureVerifier(WebhookProperties properties) {
        this.properties = properties;
        String appSecret = properties.meta().appSecret();
        this.signingKey = appSecret == null || appSecret.isEmpty()
                ? null
                : new SecretKeySpec(appSecret.getBytes(StandardCharsets.UTF_8), HMAC_ALGORITHM);
    }

    @Override
    public void verifySignature(byte[] rawBody, String signatureHeader) {
        if (!properties.meta().verifySignature()) {
            return;
        }
        if (signingKey == null) {
            // Startup validation prevents this; defence in depth against a hot config change.
            throw new InvalidSignatureException("No app secret is configured");
        }
        if (signatureHeader == null || signatureHeader.isBlank()) {
            throw new InvalidSignatureException(
                    "Missing " + properties.meta().signatureHeader() + " header");
        }
        if (!signatureHeader.startsWith(SIGNATURE_PREFIX)) {
            throw new InvalidSignatureException("Signature header is not in sha256=<hex> form");
        }

        byte[] provided;
        try {
            provided = HexFormat.of().parseHex(signatureHeader.substring(SIGNATURE_PREFIX.length()).trim());
        } catch (IllegalArgumentException e) {
            throw new InvalidSignatureException("Signature header does not contain valid hex");
        }

        byte[] expected = hmac(rawBody == null ? new byte[0] : rawBody);
        if (!MessageDigest.isEqual(expected, provided)) {
            throw new InvalidSignatureException("Signature did not match the request body");
        }
    }

    @Override
    public boolean matchesVerifyToken(String candidate) {
        if (candidate == null) {
            return false;
        }
        String expected = properties.meta().verifyToken();
        if (expected == null || expected.isEmpty()) {
            return false;
        }
        return MessageDigest.isEqual(digest(expected), digest(candidate));
    }

    /** Hex encoded HMAC of the given bytes. Exposed for tests against known Meta vectors. */
    public String hmacHex(byte[] rawBody) {
        return HexFormat.of().formatHex(hmac(rawBody));
    }

    private byte[] hmac(byte[] rawBody) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(signingKey);
            return mac.doFinal(rawBody);
        } catch (Exception e) {
            throw new IllegalStateException("Could not compute " + HMAC_ALGORITHM, e);
        }
    }

    private byte[] digest(String value) {
        try {
            return MessageDigest.getInstance(DIGEST_ALGORITHM)
                    .digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(DIGEST_ALGORITHM + " is not available", e);
        }
    }
}
