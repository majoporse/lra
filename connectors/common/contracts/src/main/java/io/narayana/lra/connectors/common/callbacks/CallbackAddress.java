/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.callbacks;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;

/**
 * The saved address the coordinator uses for one participant callback kind.
 * <p>
 * A callback address is polymorphic: each connector's contracts module owns its subtype and the
 * fields that address needs. HTTP addresses carry a URI; a Kafka address carries a service topic.
 * The {@code type} property is only the JSON discriminator, kept for persisted and join-body
 * compatibility; the code dispatches on the subtype and never on the string.
 * <p>
 * The permitted subtypes are not listed here. Each connector contributes a Jackson
 * {@code Module} that registers its subtypes; {@code ObjectMapper.findAndRegisterModules()}
 * discovers those modules through {@code META-INF/services/com.fasterxml.jackson.databind.Module}.
 * The id is the subtype's {@code @JsonTypeName}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type", visible = false)
public interface CallbackAddress {

    CallbackType operation();

    String participantId();

    /**
     * Fill in the operation for a legacy record that was persisted without one.
     */
    CallbackAddress withOperation(CallbackType operation);

    /**
     * The address a recovery URL should report: the HTTP URI when the address has one, otherwise
     * the address JSON. Each subtype that can be expressed as a URI overrides it.
     */
    default String callbackUri() {
        return toJson(this);
    }

    /**
     * Write the address as the flat persisted/join-body JSON.
     */
    static String toJson(CallbackAddress address) {
        if (address == null) {
            return null;
        }

        if (address instanceof UnknownCallbackAddress unknown) {
            return unknown.rawJson();
        }

        try {
            return Json.MAPPER.writeValueAsString(address);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize callback address", e);
        }
    }

    /**
     * Read the flat persisted/join-body JSON. A record without {@code type} is a legacy HTTP
     * address; an address whose connector is absent becomes an {@link UnknownCallbackAddress} so
     * recovery can continue and the delivery fails loudly.
     */
    static CallbackAddress fromJson(String json, CallbackType expectedOperation) {
        if (json == null) {
            return null;
        }

        CallbackAddress address;

        try {
            address = read(Json.MAPPER.readTree(json));
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot deserialize callback address", e);
        }

        return expectedOperation != null && address.operation() == null
                ? address.withOperation(expectedOperation)
                : address;
    }

    private static CallbackAddress read(JsonNode node) throws IOException {
        if (node == null || node.isNull()) {
            return null;
        }

        if (!(node instanceof ObjectNode object)) {
            throw new IllegalArgumentException("A callback address must be a JSON object");
        }

        JsonNode typeNode = object.get("type");
        boolean legacy = typeNode == null || typeNode.isNull();
        ObjectNode typed = object;

        if (legacy) {
            typed = object.deepCopy();
            typed.put("type", Json.LEGACY_DEFAULT_TYPE);
        }

        try {
            return Json.MAPPER.treeToValue(typed, CallbackAddress.class);
        } catch (InvalidTypeIdException e) {
            // Keep the raw JSON unchanged, including the absence of a type property.
            return UnknownCallbackAddress.from(legacy ? null : e.getTypeId(), object);
        }
    }

    /**
     * Holds the shared mapper. Interface fields are implicitly public, so the mapper lives in this
     * nested holder to keep it private.
     */
    final class Json {

        /**
         * Records written before the {@code type} discriminator existed were HTTP addresses.
         */
        private static final String LEGACY_DEFAULT_TYPE = "http";

        private static final ObjectMapper MAPPER = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .findAndRegisterModules();

        private Json() {
        }
    }
}
