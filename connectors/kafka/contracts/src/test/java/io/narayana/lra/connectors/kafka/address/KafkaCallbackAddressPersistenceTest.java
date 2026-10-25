/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.address;

import io.narayana.lra.connectors.common.callbacks.CallbackAddress;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KafkaCallbackAddressPersistenceTest {

    @Test
    public void roundTripsAKafkaAddress() {
        KafkaAddress address = new KafkaAddress(CallbackType.COMPENSATE, "order-service", "orders");

        CallbackAddress restored = CallbackAddress.fromJson(CallbackAddress.toJson(address), null);

        Assertions.assertEquals(address, restored);
    }

    @Test
    public void readsTheLegacyKafkaAfterOperationName() {
        String json = "{\"type\":\"kafka\",\"operation\":\"AFTER_LRA\","
                + "\"participantId\":\"order-service\",\"topic\":\"orders\"}";

        CallbackAddress address = CallbackAddress.fromJson(json, CallbackType.COMPLETE);

        Assertions.assertInstanceOf(KafkaAddress.class, address);
        Assertions.assertEquals(CallbackType.AFTER, address.operation());
        Assertions.assertEquals("orders", ((KafkaAddress) address).topic());
    }

    @Test
    public void reportsTheJsonAsTheCallbackUri() {
        Assertions.assertTrue(new KafkaAddress(CallbackType.COMPENSATE, "p", "orders")
                .callbackUri().contains("\"topic\":\"orders\""));
    }
}
