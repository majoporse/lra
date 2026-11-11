/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.Current;
import io.narayana.lra.LRAConstants;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.client.callback.CallbackReceiver;
import io.narayana.lra.connectors.common.client.callback.ParticipantCallbackDispatcher;
import io.narayana.lra.connectors.common.contracts.CompensateCallback;
import io.narayana.lra.connectors.common.contracts.ParticipantRequest;
import io.narayana.lra.connectors.kafka.contracts.CompensateKafkaCallback;
import io.narayana.lra.connectors.kafka.contracts.KafkaMessage;
import io.narayana.lra.connectors.kafka.contracts.LRAKafkaEnvelope;
import io.narayana.lra.connectors.kafka.outbound.KafkaOutbound;
import io.narayana.lra.connectors.kafka.support.LRAKafkaConstants;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KafkaInboundListenerTest {

    @Test
    public void dispatchesACallbackAndRepliesToTheCoordinator() throws Exception {
        CapturingOutbound outbound = new CapturingOutbound();
        ParticipantCallbackDispatcher dispatcher = (type, request) -> {
            Assertions.assertEquals(CallbackType.COMPENSATE, type);
            Assertions.assertEquals("participant", request.participantId);
            return new CompensateCallback.Reply(ParticipantStatus.Compensated);
        };

        String payload = callbackRequest("correlation", "participant");
        listener(outbound, dispatcher).onMessage(Message.of(payload)).toCompletableFuture().get(1, TimeUnit.SECONDS);

        Assertions.assertEquals("reply-topic", outbound.topic);
        Assertions.assertEquals(LRAKafkaConstants.TYPE_COMPENSATE_REPLY, outbound.type);

        CompensateKafkaCallback.Reply reply = (CompensateKafkaCallback.Reply) outbound.payload;
        Assertions.assertEquals("correlation", reply.getCorrelationId());
        Assertions.assertEquals(ParticipantStatus.Compensated, reply.status);
        Assertions.assertNull(reply.getError());
    }

    @Test
    public void completesACoordinatorReply() throws Exception {
        CapturingOutbound outbound = new CapturingOutbound();
        ParticipantCallbackDispatcher dispatcher = (type, request) -> {
            throw new AssertionError("a coordinator reply must not be dispatched as a callback");
        };

        String payload = "{\"correlationId\":\"correlation\",\"lraId\":\"http://lra/1\"}";
        listener(outbound, dispatcher).onMessage(Message.of(payload)).toCompletableFuture().get(1, TimeUnit.SECONDS);

        Assertions.assertEquals(payload, outbound.reply);
        Assertions.assertNull(outbound.type);
    }

    @Test
    public void reportsAMissingParticipantAsAnErrorReply() throws Exception {
        CapturingOutbound outbound = new CapturingOutbound();

        listener(outbound, (type, request) -> null)
                .onMessage(Message.of(callbackRequest("correlation", "missing")))
                .toCompletableFuture().get(1, TimeUnit.SECONDS);

        CompensateKafkaCallback.Reply reply = (CompensateKafkaCallback.Reply) outbound.payload;

        Assertions.assertNotNull(reply.getError());
        Assertions.assertNull(reply.status);
    }

    @Test
    public void makesTheCallbackLraTheEffectiveLra() throws Exception {
        URI lraId = LRAConstants.toLRAId(UUID.randomUUID());
        CapturingOutbound outbound = new CapturingOutbound();
        ParticipantCallbackDispatcher dispatcher = (type, request) -> {
            Assertions.assertEquals(lraId, Current.peek());
            return new CompensateCallback.Reply(ParticipantStatus.Compensated);
        };

        listener(outbound, dispatcher)
                .onMessage(Message.of(callbackRequest("correlation", "participant", lraId)))
                .toCompletableFuture().get(1, TimeUnit.SECONDS);

        Assertions.assertNull(Current.peek());
    }

    @Test
    public void restoresTheCallersLraAfterTheCallback() throws Exception {
        URI outer = LRAConstants.toLRAId(UUID.randomUUID());
        URI lraId = LRAConstants.toLRAId(UUID.randomUUID());
        Current.push(outer, null);
        Current.addActiveLRACache(outer);

        try {
            CapturingOutbound outbound = new CapturingOutbound();
            ParticipantCallbackDispatcher dispatcher = (type, request) -> {
                Assertions.assertEquals(lraId, Current.peek());
                return new CompensateCallback.Reply(ParticipantStatus.Compensated);
            };

            listener(outbound, dispatcher)
                    .onMessage(Message.of(callbackRequest("correlation", "participant", lraId)))
                    .toCompletableFuture().get(1, TimeUnit.SECONDS);

            Assertions.assertEquals(outer, Current.peek());
        } finally {
            Current.popAll();
            Current.removeActiveLRACache(outer);
        }
    }

    private static String callbackRequest(String correlationId, String participantId) throws Exception {
        return callbackRequest(correlationId, participantId, URI.create("http://lra/1"));
    }

    private static String callbackRequest(String correlationId, String participantId, URI lraId) throws Exception {
        ParticipantRequest request = new CompensateKafkaCallback.Request(correlationId, "reply-topic",
                participantId, lraId, null, null, null);

        return new ObjectMapper().writeValueAsString(
                new LRAKafkaEnvelope(LRAKafkaConstants.TYPE_COMPENSATE, request));
    }

    private static KafkaInboundListener listener(KafkaOutbound outbound, ParticipantCallbackDispatcher dispatcher) {
        KafkaInboundListener listener = new KafkaInboundListener();
        listener.kafkaOutbound = outbound;
        listener.callbackReceiver = new CallbackReceiver(dispatcher);
        return listener;
    }

    private static final class CapturingOutbound extends KafkaOutbound {
        private String topic;
        private String type;
        private KafkaMessage payload;
        private String reply;

        @Override
        public void send(String topic, KafkaMessage message) {
            this.topic = topic;
            this.type = message.getType();
            this.payload = message;
        }

        @Override
        public void completeReply(String rawJson) {
            reply = rawJson;
        }
    }
}
