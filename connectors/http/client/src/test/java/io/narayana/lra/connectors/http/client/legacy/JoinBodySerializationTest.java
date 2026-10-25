/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.legacy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.http.address.HttpContractAddress;
import io.narayana.lra.connectors.http.address.LegacyHttpAddress;
import io.narayana.lra.connectors.http.contracts.JoinLRAHttp;
import java.net.URI;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JoinBodySerializationTest {

    @Test
    public void writesTheAddressTypeForEveryCallback() throws Exception {
        String participantId = "participant";
        ParticipantCallbacks callbacks = new ParticipantCallbacks();
        callbacks.compensateCallback = new HttpContractAddress(CallbackType.COMPENSATE, participantId,
                URI.create("http://service/lra-callback-proxy/participant/compensate"));
        callbacks.afterCallback = LegacyHttpAddress.of(CallbackType.AFTER,
                URI.create("http://service/participant/after"), participantId);

        String json = new ObjectMapper().writeValueAsString(
                new JoinLRAHttp.Request(UUID.randomUUID(), 0L, callbacks, null, participantId));

        Assertions.assertTrue(json.contains("\"type\":\"http-contract\""), json);
        Assertions.assertTrue(json.contains("\"type\":\"http\""), json);
    }
}
