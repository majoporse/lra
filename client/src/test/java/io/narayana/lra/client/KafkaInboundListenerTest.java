/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.api.LRA;
import io.narayana.lra.callbacks.CallbackFactory;
import io.narayana.lra.callbacks.CallbackType;
import io.narayana.lra.callbacks.ParticipantCallbackDispatcher;
import io.narayana.lra.callbacks.contracts.common.CompensateCallback;
import io.narayana.lra.callbacks.contracts.common.ParticipantRequest;
import io.narayana.lra.callbacks.contracts.kafka.CompensateKafkaCallback;
import io.narayana.lra.contracts.kafka.LRAKafkaConstants;
import io.narayana.lra.contracts.kafka.LRAKafkaEnvelope;
import jakarta.enterprise.inject.Instance;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KafkaInboundListenerTest {

    private static final String SERVICE_TOPIC = "service-topic";

    private String previousTopic;

    @BeforeEach
    public void setServiceTopic() {
        previousTopic = System.getProperty(CallbackFactory.SERVICE_TOPIC_PROPERTY);
        System.setProperty(CallbackFactory.SERVICE_TOPIC_PROPERTY, SERVICE_TOPIC);
    }

    @AfterEach
    public void restoreServiceTopic() {
        if (previousTopic == null) {
            System.clearProperty(CallbackFactory.SERVICE_TOPIC_PROPERTY);
        } else {
            System.setProperty(CallbackFactory.SERVICE_TOPIC_PROPERTY, previousTopic);
        }
    }

    @Test
    public void dispatchesACallbackAndRepliesToTheCoordinator() throws Exception {
        CapturingClient client = new CapturingClient();
        ParticipantCallbackDispatcher dispatcher = (type, request) -> {
            Assertions.assertEquals(CallbackType.COMPENSATE, type);
            Assertions.assertEquals("participant", request.participantId);
            return new CompensateCallback.Reply(LRA.ParticipantOutcome.Compensated);
        };

        String payload = callbackRequest("correlation", "participant");
        listener(client, dispatcher).onMessage(Message.of(payload)).toCompletableFuture().get(1, TimeUnit.SECONDS);

        Assertions.assertNotNull(client.reply);
        ObjectMapper objectMapper = new ObjectMapper();
        LRAKafkaEnvelope replyEnvelope = objectMapper.readValue(client.reply, LRAKafkaEnvelope.class);
        Assertions.assertEquals(LRAKafkaConstants.TYPE_COMPENSATE_REPLY, replyEnvelope.type);

        CompensateKafkaCallback.Reply reply = objectMapper.convertValue(replyEnvelope.payload,
                CompensateKafkaCallback.Reply.class);
        Assertions.assertEquals("correlation", reply.getCorrelationId());
        Assertions.assertEquals(LRA.ParticipantOutcome.Compensated, reply.status);
        Assertions.assertNull(reply.getError());
    }

    @Test
    public void handsACoordinatorReplyToTheClient() throws Exception {
        CapturingClient client = new CapturingClient();
        ParticipantCallbackDispatcher dispatcher = (type, request) -> {
            throw new AssertionError("a coordinator reply must not be dispatched as a callback");
        };

        String payload = "{\"correlationId\":\"correlation\",\"lraId\":\"http://lra/1\"}";
        listener(client, dispatcher).onMessage(Message.of(payload)).toCompletableFuture().get(1, TimeUnit.SECONDS);

        Assertions.assertEquals(payload, client.handled);
        Assertions.assertNull(client.reply);
    }

    @Test
    public void reportsAMissingParticipantAsAnErrorReply() throws Exception {
        CapturingClient client = new CapturingClient();

        listener(client, (type, request) -> null)
                .onMessage(Message.of(callbackRequest("correlation", "missing")))
                .toCompletableFuture().get(1, TimeUnit.SECONDS);

        ObjectMapper objectMapper = new ObjectMapper();
        LRAKafkaEnvelope replyEnvelope = objectMapper.readValue(client.reply, LRAKafkaEnvelope.class);
        CompensateKafkaCallback.Reply reply = objectMapper.convertValue(replyEnvelope.payload,
                CompensateKafkaCallback.Reply.class);

        Assertions.assertNotNull(reply.getError());
        Assertions.assertNull(reply.status);
    }

    private static String callbackRequest(String correlationId, String participantId) throws Exception {
        ParticipantRequest request = new CompensateKafkaCallback.Request(correlationId, "reply-topic",
                participantId, URI.create("http://lra/1"), null, null, null);

        return new ObjectMapper().writeValueAsString(
                new LRAKafkaEnvelope(LRAKafkaConstants.TYPE_COMPENSATE, request));
    }

    private static KafkaInboundListener listener(KafkaLRAClient client, ParticipantCallbackDispatcher dispatcher) {
        KafkaInboundListener listener = new KafkaInboundListener();
        listener.kafkaLRAClient = client;
        listener.participantCallbackDispatcher = instance(dispatcher);
        return listener;
    }

    @SuppressWarnings("unchecked")
    private static Instance<ParticipantCallbackDispatcher> instance(ParticipantCallbackDispatcher dispatcher) {
        return (Instance<ParticipantCallbackDispatcher>) Proxy.newProxyInstance(
                KafkaInboundListenerTest.class.getClassLoader(),
                new Class<?>[] { Instance.class },
                (proxy, method, args) -> switch (method.getName()) {
                    case "isResolvable" -> true;
                    case "get" -> dispatcher;
                    case "toString" -> "test-instance";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static final class CapturingClient extends KafkaLRAClient {
        private String reply;
        private String handled;

        @Override
        public void sendToCoordinator(String envelopeJson) {
            reply = envelopeJson;
        }

        @Override
        public void handleReply(String payload) {
            handled = payload;
        }
    }
}
