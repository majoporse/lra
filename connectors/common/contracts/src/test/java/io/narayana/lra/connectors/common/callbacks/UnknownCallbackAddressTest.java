/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.callbacks;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The common module has no connector on its classpath, so every stored address must stay readable
 * as an unknown address. This is the case where a coordinator lost the connector that wrote it.
 */
public class UnknownCallbackAddressTest {

    @Test
    public void keepsAnAddressWhoseConnectorIsAbsent() {
        String json = "{\"type\":\"carrier-pigeon\",\"operation\":\"COMPENSATE\",\"participantId\":\"p\","
                + "\"coop\":\"roof\"}";

        CallbackAddress address = CallbackAddress.fromJson(json, CallbackType.COMPLETE);

        UnknownCallbackAddress unknown = Assertions.assertInstanceOf(UnknownCallbackAddress.class, address);
        Assertions.assertEquals("carrier-pigeon", unknown.type());
        Assertions.assertEquals("p", unknown.participantId());
        Assertions.assertEquals(CallbackType.COMPENSATE, unknown.operation());
        Assertions.assertEquals(json, unknown.rawJson());
    }

    @Test
    public void fillsAMissingOperationAndRoundTripsTheRawJson() {
        String json = "{\"type\":\"carrier-pigeon\",\"participantId\":\"p\"}";

        CallbackAddress address = CallbackAddress.fromJson(json, CallbackType.COMPENSATE);

        Assertions.assertEquals(CallbackType.COMPENSATE, address.operation());
        Assertions.assertTrue(CallbackAddress.toJson(address).contains("\"operation\":\"COMPENSATE\""));
    }

    @Test
    public void reportsTheRawJsonAsTheCallbackUri() {
        String json = "{\"type\":\"carrier-pigeon\",\"topic\":\"roof\"}";

        Assertions.assertEquals(json, CallbackAddress.fromJson(json, null).callbackUri());
    }

    @Test
    public void keepsAnUntypedLegacyRecordWhenNoConnectorProvidesADefault() {
        String json = "{\"operation\":\"COMPENSATE\",\"uri\":\"http://legacy/compensate\"}";

        CallbackAddress address = CallbackAddress.fromJson(json, null);

        UnknownCallbackAddress unknown = Assertions.assertInstanceOf(UnknownCallbackAddress.class, address);
        Assertions.assertNull(unknown.type());
        Assertions.assertEquals(json, unknown.rawJson());
    }
}
