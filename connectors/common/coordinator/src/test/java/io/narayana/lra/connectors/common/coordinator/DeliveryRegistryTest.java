/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.coordinator;

import io.narayana.lra.connectors.common.callbacks.CallbackAddress;
import io.narayana.lra.connectors.common.callbacks.CallbackContext;
import io.narayana.lra.connectors.common.callbacks.CallbackResult;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DeliveryRegistryTest {

    private static final TestAddress TEST = new TestAddress(CallbackType.COMPENSATE, "p");
    private static final OtherTestAddress OTHER = new OtherTestAddress(CallbackType.COMPENSATE, "p");

    @Test
    public void returnsTheDeliveryForItsAddressType() {
        CallbackDelivery<TestAddress> test = delivery(TestAddress.class, true);
        DeliveryRegistry registry = new DeliveryRegistry(List.<CallbackDelivery<?>> of(test));

        Assertions.assertSame(test, registry.forAddress(TEST));
    }

    @Test
    public void rejectsAnUnknownAddressType() {
        DeliveryRegistry registry = new DeliveryRegistry(List.<CallbackDelivery<?>> of(
                delivery(TestAddress.class, true)));

        NoCallbackDeliveryException error = Assertions.assertThrows(NoCallbackDeliveryException.class,
                () -> registry.forAddress(OTHER));

        Assertions.assertTrue(error.getMessage().contains("OtherTestAddress"));
        Assertions.assertTrue(error.getMessage().contains("TestAddress"));
    }

    @Test
    public void rejectsADisabledDelivery() {
        DeliveryRegistry registry = new DeliveryRegistry(List.<CallbackDelivery<?>> of(
                delivery(OtherTestAddress.class, false)));

        NoCallbackDeliveryException error = Assertions.assertThrows(NoCallbackDeliveryException.class,
                () -> registry.forAddress(OTHER));

        Assertions.assertTrue(error.getMessage().contains("disabled"));
    }

    @Test
    public void rejectsDuplicateAddressTypes() {
        Assertions.assertThrows(IllegalStateException.class,
                () -> new DeliveryRegistry(List.<CallbackDelivery<?>> of(
                        delivery(TestAddress.class, true), delivery(TestAddress.class, true))));
    }

    private static <A extends CallbackAddress> CallbackDelivery<A> delivery(Class<A> type, boolean enabled) {
        return new CallbackDelivery<>() {
            @Override
            public Class<A> addressType() {
                return type;
            }

            @Override
            public boolean isEnabled() {
                return enabled;
            }

            @Override
            public CallbackResult request(A address, CallbackContext context, Duration timeout) {
                return null;
            }
        };
    }

    private record TestAddress(CallbackType operation, String participantId) implements CallbackAddress {

        @Override
        public CallbackAddress withOperation(CallbackType operation) {
            return new TestAddress(operation, participantId);
        }
    }

    private record OtherTestAddress(CallbackType operation, String participantId) implements CallbackAddress {

        @Override
        public CallbackAddress withOperation(CallbackType operation) {
            return new OtherTestAddress(operation, participantId);
        }
    }
}
