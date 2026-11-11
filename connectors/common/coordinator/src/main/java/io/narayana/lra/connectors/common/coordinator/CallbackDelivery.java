/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.coordinator;

import io.narayana.lra.connectors.common.callbacks.CallbackAddress;
import io.narayana.lra.connectors.common.callbacks.CallbackContext;
import io.narayana.lra.connectors.common.callbacks.CallbackResult;
import java.time.Duration;

/**
 * The coordinator's per-address-type outbound adapter for callbacks.
 * <p>
 * The delivery works with the connector's contract request/reply types and returns the domain
 * callback result. The shared {@code CallbackOutcomeMapper} is the one place a contract reply's
 * outcome is interpreted; there is no neutral reply envelope.
 *
 * @param <A> the callback address subtype this delivery accepts
 */
public interface CallbackDelivery<A extends CallbackAddress> {

    Class<A> addressType();

    /**
     * A disabled delivery stays registered but cannot be used. The registry reports it instead of
     * silently skipping the callback.
     */
    default boolean isEnabled() {
        return true;
    }

    CallbackResult request(A address, CallbackContext context, Duration timeout);
}
