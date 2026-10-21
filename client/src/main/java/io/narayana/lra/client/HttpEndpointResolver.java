/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

import io.narayana.lra.callbacks.CallbackType;
import io.narayana.lra.callbacks.HttpCallback;
import io.narayana.lra.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.api.CallbackPaths;
import io.narayana.lra.connectors.api.ParticipantCallbackEndpointResolver;
import io.narayana.lra.connectors.api.ParticipantCallbackTarget;
import io.narayana.lra.connectors.api.ParticipantConnector;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.UriBuilder;
import java.net.URI;

/**
 * Writes the common callback contract endpoints for an HTTP participant:
 * {@code {base}/lra-callback-proxy/{participantId}/{op}}.
 */
@ApplicationScoped
@ParticipantConnector(CallbackPaths.HTTP)
public class HttpEndpointResolver implements ParticipantCallbackEndpointResolver {

    @Override
    public String protocolId() {
        return CallbackPaths.HTTP;
    }

    @Override
    public ParticipantCallbacks resolve(ParticipantCallbackTarget target) {
        if (target.baseUri() == null) {
            throw new IllegalArgumentException(
                    "The HTTP connector needs a base URI to resolve callbacks for participant '"
                            + target.participantId() + "'");
        }

        ParticipantCallbacks callbacks = new ParticipantCallbacks();
        URI base = target.baseUri();

        for (CallbackType type : target.callbackTypes()) {
            URI uri = UriBuilder.fromUri(base)
                    .path(CallbackPaths.RESOURCE_PATH)
                    .path(target.participantId())
                    .path(type.rel())
                    .build();

            callbacks.callback(type, HttpCallback.of(type, uri, target.participantId()));
        }

        return callbacks;
    }
}
