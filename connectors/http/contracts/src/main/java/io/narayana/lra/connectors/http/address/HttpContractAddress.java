/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.address;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.narayana.lra.connectors.common.callbacks.CallbackAddress;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.http.support.HttpContractPaths;
import java.net.URI;

/**
 * The common callback contract delivered over HTTP:
 * {@code {base}/lra-callback-proxy/{participantId}/{op}}.
 */
@JsonTypeName(HttpContractPaths.HTTP_CONTRACT)
public record HttpContractAddress(
        CallbackType operation,
        String participantId,
        URI uri) implements CallbackAddress {

    @Override
    public CallbackAddress withOperation(CallbackType operation) {
        return new HttpContractAddress(operation, participantId, uri);
    }

    @Override
    public String callbackUri() {
        return uri.toASCIIString();
    }
}
