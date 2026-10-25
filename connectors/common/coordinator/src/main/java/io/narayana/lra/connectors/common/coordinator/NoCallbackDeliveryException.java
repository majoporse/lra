/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.coordinator;

import io.narayana.lra.connectors.common.callbacks.CallbackAddress;
import java.util.Set;

/**
 * Thrown when a callback address has no registered, enabled delivery.
 */
public class NoCallbackDeliveryException extends RuntimeException {

    public NoCallbackDeliveryException(Class<? extends CallbackAddress> addressType,
            Set<Class<? extends CallbackAddress>> registeredTypes, boolean disabled) {
        super("No callback delivery registered for address type '" + name(addressType) + "'"
                + (disabled ? " (the connector is disabled)" : "")
                + ". Registered address types: " + registeredTypes.stream()
                        .map(NoCallbackDeliveryException::name)
                        .sorted()
                        .toList());
    }

    private static String name(Class<? extends CallbackAddress> type) {
        return type == null ? "null" : type.getSimpleName();
    }
}
