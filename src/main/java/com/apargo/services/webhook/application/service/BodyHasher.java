package com.apargo.services.webhook.application.service;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.springframework.stereotype.Component;

/** SHA-256 of the raw POST body, hex encoded. This value is the dedupe key and {@code bodyHash}. */
@Component
public class BodyHasher {

    private static final String ALGORITHM = "SHA-256";

    public String hash(byte[] rawBody) {
        try {
            MessageDigest digest = MessageDigest.getInstance(ALGORITHM);
            return HexFormat.of().formatHex(digest.digest(rawBody == null ? new byte[0] : rawBody));
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by the JDK specification; unreachable on any supported runtime.
            throw new IllegalStateException(ALGORITHM + " is not available", e);
        }
    }
}
