/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.callback;

import io.narayana.lra.connectors.common.callbacks.CallbackContext;
import io.narayana.lra.connectors.common.callbacks.CallbackResult;
import io.narayana.lra.connectors.common.callbacks.CallbackStatus;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.coordinator.CallbackDelivery;
import io.narayana.lra.connectors.common.coordinator.DeliveryRegistry;
import io.narayana.lra.connectors.http.address.HttpContractAddress;
import io.narayana.lra.connectors.http.address.LegacyHttpAddress;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CallbackSenderTest {

    private static final HttpContractAddress ADDRESS = new HttpContractAddress(CallbackType.COMPENSATE, "p",
            URI.create("http://participant/compensate"));
    private static final CallbackContext CONTEXT = new CallbackContext("lra://1", null, null, null);

    @Test
    public void returnsTheDeliveryResult() {
        CallbackDelivery<HttpContractAddress> delivery = new CallbackDelivery<>() {
            @Override
            public Class<HttpContractAddress> addressType() {
                return HttpContractAddress.class;
            }

            @Override
            public CallbackResult request(HttpContractAddress address, CallbackContext context, Duration timeout) {
                Assertions.assertSame(ADDRESS, address);
                Assertions.assertSame(CONTEXT, context);

                return new CallbackResult(CallbackStatus.ACCEPTED);
            }
        };

        CallbackSender sender = new CallbackSender(new DeliveryRegistry(List.<CallbackDelivery<?>> of(delivery)));
        CallbackResult result = sender.send(ADDRESS, CONTEXT);

        Assertions.assertEquals(CallbackStatus.ACCEPTED, result.status);
    }

    @Test
    public void reportsAMissingDeliveryAsAnError() {
        CallbackDelivery<LegacyHttpAddress> legacy = new CallbackDelivery<>() {
            @Override
            public Class<LegacyHttpAddress> addressType() {
                return LegacyHttpAddress.class;
            }

            @Override
            public CallbackResult request(LegacyHttpAddress address, CallbackContext context, Duration timeout) {
                return new CallbackResult(CallbackStatus.OK);
            }
        };

        CallbackSender sender = new CallbackSender(new DeliveryRegistry(List.<CallbackDelivery<?>> of(legacy)));
        CallbackResult result = sender.send(ADDRESS, CONTEXT);

        Assertions.assertEquals(CallbackStatus.ERROR, result.status);
        Assertions.assertTrue(result.body.contains("HttpContractAddress"));
    }

    @Test
    public void treatsAMissingAddressAsNothingToDo() {
        CallbackSender sender = new CallbackSender(new DeliveryRegistry(List.of()));

        Assertions.assertEquals(CallbackStatus.OK, sender.send(null, CONTEXT).status);
    }
}
