/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.outbound;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.api.LRA;
import io.narayana.lra.connectors.kafka.contracts.CompensateKafkaCallback;
import io.narayana.lra.connectors.kafka.contracts.StatusLRAKafka;
import io.narayana.lra.connectors.kafka.support.LRAKafkaConstants;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KafkaOutboundTest {

    @Test
    public void sendsAnEnvelopeOnTheRequestedTopic() throws Exception {
        RecordingEmitter emitter = new RecordingEmitter();
        KafkaOutbound outbound = new KafkaOutbound();
        outbound.emitter = emitter;

        outbound.send("participant-topic", new CompensateKafkaCallback.Request("c1", "reply-topic",
                URI.create("http://lra/1"), null, null, null));

        JsonNode envelope = new ObjectMapper().readTree(emitter.payloads.get(0));
        Assertions.assertEquals(LRAKafkaConstants.TYPE_COMPENSATE, envelope.get("type").asText());
        Assertions.assertEquals("c1", envelope.get("payload").get("correlationId").asText());
        Assertions.assertEquals("participant-topic", emitter.topics.get(0));
    }

    @Test
    public void completesARequestFromACallbackReplyEnvelope() throws Exception {
        RecordingEmitter emitter = new RecordingEmitter();
        KafkaOutbound outbound = new KafkaOutbound();
        outbound.emitter = emitter;

        CompensateKafkaCallback.Request request = new CompensateKafkaCallback.Request("c1", "reply-topic",
                URI.create("http://lra/1"), null, null, null);

        CompletableFuture<CompensateKafkaCallback.Reply> replyFuture = CompletableFuture
                .supplyAsync(() -> outbound.request("participant-topic", request, Duration.ofSeconds(2)));

        Assertions.assertTrue(emitter.sent.await(1, TimeUnit.SECONDS));

        outbound.completeReply("{\"type\":\"compensate-reply\",\"payload\":"
                + "{\"correlationId\":\"c1\",\"status\":\"Compensated\"}}");

        CompensateKafkaCallback.Reply reply = replyFuture.get(2, TimeUnit.SECONDS);
        Assertions.assertEquals("c1", reply.getCorrelationId());
        Assertions.assertEquals(LRA.ParticipantOutcome.Compensated, reply.getStatus());
    }

    @Test
    public void completesARequestFromABareCoordinatorReply() throws Exception {
        RecordingEmitter emitter = new RecordingEmitter();
        KafkaOutbound outbound = new KafkaOutbound();
        outbound.emitter = emitter;

        StatusLRAKafka.Request request = new StatusLRAKafka.Request("c2", "service-topic", UUID.randomUUID());

        CompletableFuture<StatusLRAKafka.Reply> replyFuture = CompletableFuture
                .supplyAsync(() -> outbound.request("coordinator-topic", request, Duration.ofSeconds(2)));

        Assertions.assertTrue(emitter.sent.await(1, TimeUnit.SECONDS));

        outbound.completeReply("{\"correlationId\":\"c2\",\"status\":\"Active\"}");

        StatusLRAKafka.Reply reply = replyFuture.get(2, TimeUnit.SECONDS);
        Assertions.assertEquals("c2", reply.getCorrelationId());
        Assertions.assertEquals(LRAStatus.Active, reply.status);
    }

    private static final class RecordingEmitter implements Emitter<String> {

        private final List<String> payloads = Collections.synchronizedList(new ArrayList<>());
        private final List<String> topics = Collections.synchronizedList(new ArrayList<>());
        private final CountDownLatch sent = new CountDownLatch(1);

        @Override
        public CompletionStage<Void> send(String payload) {
            payloads.add(payload);
            sent.countDown();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public <M extends Message<? extends String>> void send(M message) {
            topics.add(message.getMetadata(OutgoingKafkaRecordMetadata.class)
                    .map(OutgoingKafkaRecordMetadata::getTopic)
                    .orElse(null));
            send(message.getPayload());
        }

        @Override
        public void complete() {
        }

        @Override
        public void error(Exception e) {
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean hasRequests() {
            return false;
        }
    }
}
