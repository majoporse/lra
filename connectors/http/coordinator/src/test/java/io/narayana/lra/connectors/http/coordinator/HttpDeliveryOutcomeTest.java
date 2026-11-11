/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.coordinator;

import io.narayana.lra.connectors.common.callbacks.CallbackStatus;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.http.address.LegacyHttpAddress;
import java.net.URI;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HttpDeliveryOutcomeTest {

    @Test
    public void mapsTheLegacyWireStatus() {
        Assertions.assertEquals(CallbackStatus.OK, LegacyHttpDelivery.mapStatus(200));
        Assertions.assertEquals(CallbackStatus.OK, LegacyHttpDelivery.mapStatus(204));
        Assertions.assertEquals(CallbackStatus.ACCEPTED, LegacyHttpDelivery.mapStatus(202));
        Assertions.assertEquals(CallbackStatus.GONE, LegacyHttpDelivery.mapStatus(410));
        Assertions.assertEquals(CallbackStatus.FAILED, LegacyHttpDelivery.mapStatus(409));
        Assertions.assertEquals(CallbackStatus.ERROR, LegacyHttpDelivery.mapStatus(404));
        Assertions.assertEquals(CallbackStatus.ERROR, LegacyHttpDelivery.mapStatus(500));
    }

    @Test
    public void mapsALocationHeaderToALegacyStatusAddress() {
        LegacyHttpAddress address = new LegacyHttpAddress(CallbackType.COMPENSATE, "p",
                URI.create("http://participant/compensate"), "PUT", "ACTIVE");

        LegacyHttpAddress updated = LegacyHttpDelivery.updatedAddress(address, "http://participant/status");

        Assertions.assertEquals(CallbackType.STATUS, updated.operation());
        Assertions.assertEquals("http://participant/status", updated.uri().toASCIIString());
        Assertions.assertEquals("PUT", updated.method());
        Assertions.assertEquals("ACTIVE", updated.contextType());
    }
}
