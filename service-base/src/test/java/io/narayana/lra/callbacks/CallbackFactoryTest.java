/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.callbacks;

import io.narayana.lra.LRAConstants;
import io.narayana.lra.api.LRA;
import io.narayana.lra.callbacks.contracts.kafka.AfterLRAKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.CompensateKafkaCallback;
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
        Assertions.assertInstanceOf(LegacyHttpCallback.class, callback);
    }

    @Test
    public void httpKeepsTheExplicitUri() {
        setProtocol(Protocol.HTTP);
        try {
            CallbackFactory factory = new CallbackFactory();
            URI uri = URI.create("http://localhost:8080/participant/compensate");
            LegacyHttpCallback callback = (LegacyHttpCallback) factory.of(CallbackType.COMPENSATE, uri);
            Assertions.assertEquals(uri.toASCIIString(), callback.getUri());
        } finally {
            clearProtocol();
        }
    }

    @Test
    public void contractFactoryProducesContractHttpCallbacks() {
        setProtocol(Protocol.HTTP);
        try {
            URI uri = URI.create("http://localhost:8080/lra-callback-proxy/com.example.Participant/compensate");
            CallbackFactory factory = CallbackFactory.forContractParticipant("com.example.Participant");
            LRACallback callback = factory.of(CallbackType.COMPENSATE, uri);

            Assertions.assertInstanceOf(HttpCallback.class, callback);
            HttpCallback httpCallback = (HttpCallback) callback;
            Assertions.assertEquals(uri.toASCIIString(), httpCallback.getUri());
            Assertions.assertEquals("com.example.Participant", httpCallback.getParticipantId());
            Assertions.assertEquals(CallbackType.COMPENSATE, httpCallback.getOperation());
        } finally {
            clearProtocol();
        }
    }

    @Test
    public void contractFactoryStillSelectsKafkaWhenConfigured() {
        setProtocol(Protocol.KAFKA);
        setServiceTopic("lra-service-test");
        try {
            CallbackFactory factory = CallbackFactory.forContractParticipant("com.example.Participant");
            LRACallback callback = factory.of(CallbackType.COMPENSATE,
                    URI.create("http://ignored/participant/compensate"));

            Assertions.assertInstanceOf(KafkaCallback.class, callback);
            Assertions.assertEquals("com.example.Participant", ((KafkaCallback) callback).getParticipantId());
        } finally {
            clearProtocol();
            clearServiceTopic();
        }
    }

    @Test
    public void kafkaProducesKafkaCallbacksOnTheServiceTopic() {
        setProtocol(Protocol.KAFKA);
        setServiceTopic("lra-service-test");
        try {
            CallbackFactory factory = new CallbackFactory("com.example.Participant");
            // the explicit URI is ignored for Kafka callbacks
            LRACallback callback = factory.of(CallbackType.COMPENSATE,
                    URI.create("http://ignored/participant/compensate"));
            Assertions.assertInstanceOf(KafkaCallback.class, callback);
            Assertions.assertEquals("lra-service-test", ((KafkaCallback) callback).getTopic());
            Assertions.assertEquals("com.example.Participant", ((KafkaCallback) callback).getParticipantId());
        } finally {
            clearProtocol();
            clearServiceTopic();
        }
    }

    @Test
    public void kafkaRequiresAServiceTopic() {
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
            LegacyHttpCallback callback = (LegacyHttpCallback) factory.of(CallbackType.COMPLETE, "/complete");
            Assertions.assertEquals("http://lra-host:8080/complete", callback.getUri());
        } finally {
            clearProtocol();
            System.clearProperty(LRAConstants.NARAYANA_LRA_BASE_URI_PROPERTY_NAME);
        }
    }

    @Test
    public void kafkaRequestCarriesTheParticipantId() {
        KafkaCallback callback = KafkaCallback.compensateCallback("service-topic", "com.example.Participant");

        var request = (CompensateKafkaCallback.Request) callback.newRequest(
                new CallbackContext("http://lra/1", null, "http://recovery/1", "data"), "correlation", "reply-topic");

        Assertions.assertEquals("com.example.Participant", request.participantId);
        Assertions.assertEquals("correlation", request.getCorrelationId());
        Assertions.assertEquals("reply-topic", request.getReplyTopic());
    }

    @Test
    public void kafkaAfterLRAUsesTheTransportNeutralEndStatus() {
        KafkaCallback callback = KafkaCallback.afterCallback("service-topic", "com.example.Participant");

        var request = (AfterLRAKafkaCallback.Request) callback.newRequest(
                new CallbackContext("http://lra/1", null, null, null, "Closed"), "correlation", "reply-topic");

        Assertions.assertEquals(LRA.TransactionStatus.Closed, request.endStatus);
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

    private static void setServiceTopic(String topic) {
        System.setProperty(CallbackFactory.SERVICE_TOPIC_PROPERTY, topic);
    }

    private static void clearServiceTopic() {
        System.clearProperty(CallbackFactory.SERVICE_TOPIC_PROPERTY);
    }
}
