package com.apargo.services.webhook.application.service;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Null-safe reads over Meta payloads.
 *
 * <p>Meta adds keys between versions without notice, so every read here tolerates a missing node,
 * an explicit null, and a node of an unexpected type. A strict read turns a new Meta field into a
 * 500 and, eventually, a disabled subscription.
 */
final class JsonNodes {

    private JsonNodes() {
    }

    /** @return the text at {@code node.fieldName}, or null when missing, null, blank or non-scalar */
    static String text(JsonNode node, String fieldName) {
        if (node == null) {
            return null;
        }
        return textOf(node.path(fieldName));
    }

    /** @return the node's text, or null when missing, null, blank or non-scalar */
    static String textOf(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull() || !node.isValueNode()) {
            return null;
        }
        String value = node.asText();
        return value == null || value.isBlank() ? null : value;
    }

    /** @return the named array, or a missing node — never null, so callers can chain safely */
    static JsonNode array(JsonNode node, String fieldName) {
        if (node == null) {
            return com.fasterxml.jackson.databind.node.MissingNode.getInstance();
        }
        JsonNode array = node.path(fieldName);
        return array.isArray() ? array : com.fasterxml.jackson.databind.node.MissingNode.getInstance();
    }
}
