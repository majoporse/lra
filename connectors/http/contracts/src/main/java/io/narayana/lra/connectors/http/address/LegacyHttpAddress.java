/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.address;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.narayana.lra.connectors.common.callbacks.CallbackAddress;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import java.net.URI;

/**
 * A legacy {@code @Path} participant callback. The method and context type are part of the address
 * because the legacy protocol allowed the participant to choose them.
 */
@JsonTypeName("http")
public record LegacyHttpAddress(
        CallbackType operation,
        String participantId,
        URI uri,
        String method,
        String contextType) implements CallbackAddress {

    public LegacyHttpAddress {
        method = method == null ? "PUT" : method;
        contextType = contextType == null ? "ACTIVE" : contextType;
    }

    /**
     * Build a legacy address with the method and context type the legacy protocol requires for the
     * callback kind.
     */
    public static LegacyHttpAddress of(CallbackType type, URI uri) {
        return of(type, uri, null);
    }

    public static LegacyHttpAddress of(CallbackType type, URI uri, String participantId) {
        String method = switch (type) {
            case COMPENSATE, COMPLETE, AFTER -> "PUT";
            case STATUS -> "GET";
            case FORGET -> "DELETE";
        };
        String contextType = type == CallbackType.AFTER ? "ENDED" : "ACTIVE";

        return new LegacyHttpAddress(type, participantId, uri, method, contextType);
    }

    @Override
    public CallbackAddress withOperation(CallbackType operation) {
        return new LegacyHttpAddress(operation, participantId, uri, method, contextType);
    }

    @Override
    public String callbackUri() {
        return uri.toASCIIString();
    }
}
