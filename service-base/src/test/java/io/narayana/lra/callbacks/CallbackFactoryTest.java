/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.callbacks;

import io.narayana.lra.LRAConstants;
import java.net.URI;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CallbackFactoryTest {

    @Test
    public void defaultsToHttp() {
        CallbackFactory factory = new CallbackFactory();
        LRACallback callback = factory.of(CallbackType.COMPENSATE,
                URI.create("http://localhost:8080/participant/compensate"));
        Assertions.assertInstanceOf(HttpCallback.class, callback);
    }

    @Test
    public void httpKeepsTheExplicitUri() {
        setProtocol(Protocol.HTTP);
        try {
            CallbackFactory factory = new CallbackFactory();
            URI uri = URI.create("http://localhost:8080/participant/compensate");
            HttpCallback callback = (HttpCallback) factory.of(CallbackType.COMPENSATE, uri);
            Assertions.assertEquals(uri.toASCIIString(), callback.getUri());
        } finally {
            clearProtocol();
        }
    }

    @Test
    public void kafkaProducesKafkaCallbacksOnTheConfiguredTopic() {
        setProtocol(Protocol.KAFKA);
        setCallbackTopic("lra-callback-test");
        try {
            CallbackFactory factory = new CallbackFactory();
            // the explicit URI is ignored for Kafka callbacks
            LRACallback callback = factory.of(CallbackType.COMPENSATE,
                    URI.create("http://ignored/participant/compensate"));
            Assertions.assertInstanceOf(KafkaCallback.class, callback);
            Assertions.assertEquals("lra-callback-test", ((KafkaCallback) callback).getTopic());
        } finally {
            clearProtocol();
            clearCallbackTopic();
        }
    }

    @Test
    public void kafkaRequiresACallbackTopic() {
        setProtocol(Protocol.KAFKA);
        try {
            Assertions.assertThrows(NoSuchElementException.class, CallbackFactory::new);
        } finally {
            clearProtocol();
        }
    }

    @Test
    public void unknownProtocolFails() {
        setProtocol("carrier-pigeon");
        try {
            Assertions.assertThrows(IllegalStateException.class, CallbackFactory::new);
        } finally {
            clearProtocol();
        }
    }

    @Test
    public void pathVariantUsesTheConfiguredBaseUri() {
        setProtocol(Protocol.HTTP);
        System.setProperty(LRAConstants.NARAYANA_LRA_BASE_URI_PROPERTY_NAME, "http://lra-host:8080");
        try {
            CallbackFactory factory = new CallbackFactory();
            HttpCallback callback = (HttpCallback) factory.of(CallbackType.COMPLETE, "/complete");
            Assertions.assertEquals("http://lra-host:8080/complete", callback.getUri());
        } finally {
            clearProtocol();
            System.clearProperty(LRAConstants.NARAYANA_LRA_BASE_URI_PROPERTY_NAME);
        }
    }

    private static void setProtocol(Protocol protocol) {
        setProtocol(protocol.value());
    }

    private static void setProtocol(String protocol) {
        System.setProperty(CallbackFactory.PROTOCOL_PROPERTY, protocol);
    }

    private static void clearProtocol() {
        System.clearProperty(CallbackFactory.PROTOCOL_PROPERTY);
    }

    private static void setCallbackTopic(String topic) {
        System.setProperty(CallbackFactory.CALLBACK_TOPIC_PROPERTY, topic);
    }

    private static void clearCallbackTopic() {
        System.clearProperty(CallbackFactory.CALLBACK_TOPIC_PROPERTY);
    }
}
