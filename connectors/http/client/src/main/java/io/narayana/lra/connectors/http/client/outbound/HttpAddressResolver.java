/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.outbound;

import io.narayana.lra.connectors.common.callbacks.CallbackAddress;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.client.CallbackPaths;
import io.narayana.lra.connectors.common.client.ParticipantCallbackAddressResolver;
import io.narayana.lra.connectors.common.client.ParticipantCallbackTarget;
import io.narayana.lra.connectors.common.client.ParticipantConnector;
import io.narayana.lra.connectors.http.address.HttpContractAddress;
import io.narayana.lra.connectors.http.address.LegacyHttpAddress;
import io.narayana.lra.connectors.http.support.HttpContractPaths;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.UriBuilder;
import java.net.URI;

/**
 * Writes the common callback contract endpoints for an HTTP participant:
 * {@code {base}/lra-callback-proxy/{participantId}/{op}}.
 */
@ApplicationScoped
@ParticipantConnector(CallbackPaths.HTTP)
public class HttpAddressResolver implements ParticipantCallbackAddressResolver {

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
                    .path(HttpContractPaths.RESOURCE_PATH)
                    .path(target.participantId())
                    .path(type.rel())
                    .build();

            callbacks.callback(type, new HttpContractAddress(type, target.participantId(), uri));
        }

        return callbacks;
    }

    @Override
    public CallbackAddress address(CallbackType type, String participantId, URI uri) {
        return LegacyHttpAddress.of(type, uri, participantId);
    }
}
