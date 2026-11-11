/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.callback;

import io.narayana.lra.LRAConstants;
import io.narayana.lra.connectors.common.callbacks.CallbackAddress;
import io.narayana.lra.connectors.common.callbacks.CallbackContext;
import io.narayana.lra.connectors.common.callbacks.CallbackResult;
import io.narayana.lra.connectors.common.callbacks.CallbackStatus;
import io.narayana.lra.connectors.common.coordinator.CallbackDelivery;
import io.narayana.lra.connectors.common.coordinator.DeliveryRegistry;
import io.narayana.lra.connectors.common.coordinator.NoCallbackDeliveryException;
import io.narayana.lra.logging.LRALogger;
import java.time.Duration;

/**
 * The one coordinator-side callback handler.
 * <p>
 * It selects the delivery by the address subtype, applies the timeout policy and returns the
 * delivery's result. It does not build requests, parse replies or switch on a protocol string.
 */
public class CallbackSender {

    private final DeliveryRegistry registry;

    public CallbackSender(DeliveryRegistry registry) {
        this.registry = registry;
    }

    public CallbackResult send(CallbackAddress address, CallbackContext context) {
        if (address == null) {
            return new CallbackResult(CallbackStatus.OK);
        }

        try {
            CallbackDelivery<CallbackAddress> delivery = registry.forAddress(address);

            return delivery.request(address, context,
                    Duration.ofSeconds(LRAConstants.PARTICIPANT_TIMEOUT));
        } catch (NoCallbackDeliveryException e) {
            LRALogger.logger.errorf("Cannot deliver LRA callback for %s: %s", address.getClass().getSimpleName(),
                    e.getMessage());

            return new CallbackResult(CallbackStatus.ERROR, e.getMessage());
        }
    }
}
