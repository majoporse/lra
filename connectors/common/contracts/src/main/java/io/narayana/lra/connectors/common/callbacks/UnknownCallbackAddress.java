/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.callbacks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Objects;

/**
 * A persisted callback address whose connector is not on the classpath.
 * <p>
 * The record stays readable during recovery; the registry has no delivery for it, so the callback
 * fails loudly and the participant stays pending instead of the whole LRA record failing to load.
 * The raw JSON is preserved so the record can be written back unchanged.
 */
public final class UnknownCallbackAddress implements CallbackAddress {

    private final String type;
    private final CallbackType operation;
    private final String participantId;
    private final ObjectNode raw;

    private UnknownCallbackAddress(String type, CallbackType operation, String participantId, ObjectNode raw) {
        this.type = type;
        this.operation = operation;
        this.participantId = participantId;
        this.raw = raw;
    }

    static UnknownCallbackAddress from(String type, ObjectNode raw) {
        JsonNode operationNode = raw.get("operation");
        JsonNode participantIdNode = raw.get("participantId");

        return new UnknownCallbackAddress(
                type,
                operationNode == null || operationNode.isNull() ? null : CallbackType.from(operationNode.asText()),
                participantIdNode == null || participantIdNode.isNull() ? null : participantIdNode.asText(),
                raw.deepCopy());
    }

    /**
     * @return the JSON discriminator this record was stored with (nullable)
     */
    public String type() {
        return type;
    }

    @Override
    public CallbackType operation() {
        return operation;
    }

    @Override
    public String participantId() {
        return participantId;
    }

    @Override
    public CallbackAddress withOperation(CallbackType operation) {
        ObjectNode updated = raw.deepCopy();
        updated.put("operation", operation == null ? null : operation.name());

        return new UnknownCallbackAddress(type, operation, participantId, updated);
    }

    /**
     * @return the stored JSON, unchanged
     */
    public String rawJson() {
        return raw.toString();
    }

    @Override
    public String callbackUri() {
        return rawJson();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof UnknownCallbackAddress other)) {
            return false;
        }

        return Objects.equals(type, other.type) && Objects.equals(rawJson(), other.rawJson());
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, rawJson());
    }

    @Override
    public String toString() {
        return "UnknownCallbackAddress{type='" + type + "', raw=" + rawJson() + '}';
    }
}
