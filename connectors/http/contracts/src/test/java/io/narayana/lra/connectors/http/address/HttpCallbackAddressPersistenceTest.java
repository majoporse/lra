/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.address;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.connectors.common.callbacks.CallbackAddress;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import java.net.URI;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HttpCallbackAddressPersistenceTest {

    @Test
    public void writesTheContractAddressWithTheTypeDiscriminator() {
        HttpContractAddress address = new HttpContractAddress(CallbackType.COMPENSATE, "order-service",
                URI.create("http://order/lra-callback-proxy/order-service/compensate"));

        String json = CallbackAddress.toJson(address);

        Assertions.assertTrue(json.contains("\"type\":\"http-contract\""));
        Assertions.assertTrue(json.contains("\"operation\":\"COMPENSATE\""));
        Assertions.assertTrue(json.contains("\"participantId\":\"order-service\""));
        Assertions.assertTrue(json.contains("\"uri\":\"http://order/lra-callback-proxy/order-service/compensate\""));
    }

    @Test
    public void roundTripsALegacyAddress() {
        LegacyHttpAddress address = new LegacyHttpAddress(CallbackType.STATUS, null,
                URI.create("http://legacy/status"), "GET", "ACTIVE");

        CallbackAddress restored = CallbackAddress.fromJson(CallbackAddress.toJson(address),
                CallbackType.STATUS);

        Assertions.assertEquals(address, restored);
    }

    @Test
    public void fillsTheOperationOfALegacyRecordThatHasNone() {
        String json = "{\"type\":\"http\",\"uri\":\"http://legacy/compensate\",\"method\":\"PUT\","
                + "\"contextType\":\"ACTIVE\"}";

        CallbackAddress address = CallbackAddress.fromJson(json, CallbackType.COMPENSATE);

        Assertions.assertInstanceOf(LegacyHttpAddress.class, address);
        Assertions.assertEquals(CallbackType.COMPENSATE, address.operation());
        Assertions.assertEquals("http://legacy/compensate", ((LegacyHttpAddress) address).uri().toASCIIString());
    }

    @Test
    public void readsALegacyRecordWithoutATypeDiscriminator() {
        String json = "{\"operation\":\"COMPENSATE\",\"uri\":\"http://legacy/compensate\"}";

        CallbackAddress address = CallbackAddress.fromJson(json, null);

        Assertions.assertInstanceOf(LegacyHttpAddress.class, address);
    }

    @Test
    public void reportsTheUriForHttpAddresses() {
        Assertions.assertEquals("http://participant/compensate",
                new LegacyHttpAddress(CallbackType.COMPENSATE, null, URI.create("http://participant/compensate"),
                        "PUT", "ACTIVE").callbackUri());
    }

    @Test
    public void writesWithAPlainJacksonMapper() throws Exception {
        String json = new ObjectMapper().writeValueAsString(callbacks());

        Assertions.assertTrue(json.contains("\"type\":\"http-contract\""));
    }

    @Test
    public void readsWithDiscoveredModules() throws Exception {
        String json = "{\"compensateCallback\":{\"type\":\"http-contract\",\"operation\":\"COMPENSATE\","
                + "\"participantId\":\"p\",\"uri\":\"http://participant/lra-callback-proxy/p/compensate\"}}";

        ParticipantCallbacks callbacks = new ObjectMapper().findAndRegisterModules()
                .readValue(json, ParticipantCallbacks.class);

        Assertions.assertInstanceOf(HttpContractAddress.class, callbacks.compensateCallback);
    }

    @Test
    public void roundTripsWithDiscoveredModules() throws Exception {
        ParticipantCallbacks callbacks = callbacks();
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

        String json = mapper.writeValueAsString(callbacks);

        Assertions.assertEquals(callbacks.compensateCallback,
                mapper.readValue(json, ParticipantCallbacks.class).compensateCallback);
    }

    private static ParticipantCallbacks callbacks() {
        ParticipantCallbacks callbacks = new ParticipantCallbacks();
        callbacks.compensateCallback = new HttpContractAddress(CallbackType.COMPENSATE, "p",
                URI.create("http://participant/lra-callback-proxy/p/compensate"));

        return callbacks;
    }
}
