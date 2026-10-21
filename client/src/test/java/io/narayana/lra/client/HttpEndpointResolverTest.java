/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

import io.narayana.lra.callbacks.CallbackType;
import io.narayana.lra.callbacks.HttpCallback;
import io.narayana.lra.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.api.ParticipantCallbackTarget;
import java.net.URI;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HttpEndpointResolverTest {

    @Test
    public void writesTheContractEndpoints() {
        ParticipantCallbacks callbacks = new HttpEndpointResolver().resolve(new ParticipantCallbackTarget(
                "order-service",
                Set.of(CallbackType.COMPENSATE, CallbackType.AFTER),
                URI.create("http://order:8080/")));

        HttpCallback compensate = (HttpCallback) callbacks.callback(CallbackType.COMPENSATE);
        HttpCallback after = (HttpCallback) callbacks.callback(CallbackType.AFTER);

        Assertions.assertEquals(
                "http://order:8080/lra-callback-proxy/order-service/compensate",
                compensate.getUri());
        Assertions.assertEquals("order-service", compensate.getParticipantId());
        Assertions.assertEquals("http://order:8080/lra-callback-proxy/order-service/after", after.getUri());
        Assertions.assertNull(callbacks.callback(CallbackType.COMPLETE));
    }

    @Test
    public void rejectsAMissingBaseUri() {
        HttpEndpointResolver resolver = new HttpEndpointResolver();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> resolver.resolve(new ParticipantCallbackTarget("order-service",
                        Set.of(CallbackType.COMPENSATE), null)));
    }

    @Test
    public void hasTheHttpProtocolId() {
        Assertions.assertEquals("http", new HttpEndpointResolver().protocolId());
    }
}
