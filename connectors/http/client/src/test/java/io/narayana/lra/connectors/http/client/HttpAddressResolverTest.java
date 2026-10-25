/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client;

import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.client.ParticipantCallbackTarget;
import io.narayana.lra.connectors.http.address.HttpContractAddress;
import io.narayana.lra.connectors.http.client.outbound.HttpAddressResolver;
import java.net.URI;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HttpAddressResolverTest {

    @Test
    public void writesTheContractAddresses() {
        ParticipantCallbacks callbacks = new HttpAddressResolver().resolve(new ParticipantCallbackTarget(
                "order-service",
                Set.of(CallbackType.COMPENSATE, CallbackType.AFTER),
                URI.create("http://order:8080/")));

        HttpContractAddress compensate = (HttpContractAddress) callbacks.callback(CallbackType.COMPENSATE);
        HttpContractAddress after = (HttpContractAddress) callbacks.callback(CallbackType.AFTER);

        Assertions.assertEquals("http://order:8080/lra-callback-proxy/order-service/compensate",
                compensate.uri().toASCIIString());
        Assertions.assertEquals("order-service", compensate.participantId());
        Assertions.assertEquals("http://order:8080/lra-callback-proxy/order-service/after",
                after.uri().toASCIIString());
        Assertions.assertNull(callbacks.callback(CallbackType.COMPLETE));
    }

    @Test
    public void rejectsAMissingBaseUri() {
        HttpAddressResolver resolver = new HttpAddressResolver();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> resolver.resolve(new ParticipantCallbackTarget("order-service",
                        Set.of(CallbackType.COMPENSATE), null)));
    }

    @Test
    public void hasTheHttpProtocolId() {
        Assertions.assertEquals("http", new HttpAddressResolver().protocolId());
    }
}
