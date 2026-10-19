/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.api;

import io.narayana.lra.api.LRA;
import io.narayana.lra.callbacks.contracts.kafka.CompleteKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.KafkaCallbackReply;
import io.narayana.lra.contracts.kafka.LRAKafkaConstants;
import io.narayana.lra.contracts.kafka.LRAKafkaEnvelope;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KafkaCallbackSenderImplTest {

    @SuppressWarnings("unchecked")
    @Test
    public void completesThePendingRequestByCorrelationId() throws Exception {
        KafkaCallbackSenderImpl sender = new KafkaCallbackSenderImpl();

        Field field = KafkaCallbackSenderImpl.class.getDeclaredField("pendingReplies");
        field.setAccessible(true);
        Map<String, CompletableFuture<KafkaCallbackReply>> pending = (Map<String, CompletableFuture<KafkaCallbackReply>>) field
                .get(sender);

        CompletableFuture<KafkaCallbackReply> future = new CompletableFuture<>();
        pending.put("correlation", future);

        LRAKafkaEnvelope envelope = new LRAKafkaEnvelope(LRAKafkaConstants.TYPE_COMPLETE_REPLY,
                Map.of("correlationId", "correlation", "status", "Completed"));
        sender.handleReply(envelope);

        KafkaCallbackReply reply = future.get(1, TimeUnit.SECONDS);
        Assertions.assertInstanceOf(CompleteKafkaCallback.Reply.class, reply);
        Assertions.assertEquals(LRA.ParticipantOutcome.Completed, reply.getStatus());
        Assertions.assertFalse(pending.containsKey("correlation"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void ignoresRepliesWithAnUnknownCorrelationId() throws Exception {
        KafkaCallbackSenderImpl sender = new KafkaCallbackSenderImpl();

        Field field = KafkaCallbackSenderImpl.class.getDeclaredField("pendingReplies");
        field.setAccessible(true);
        Map<String, CompletableFuture<KafkaCallbackReply>> pending = (Map<String, CompletableFuture<KafkaCallbackReply>>) field
                .get(sender);

        LRAKafkaEnvelope envelope = new LRAKafkaEnvelope(LRAKafkaConstants.TYPE_COMPLETE_REPLY,
                Map.of("correlationId", "missing", "status", "Completed"));
        sender.handleReply(envelope);

        Assertions.assertTrue(pending.isEmpty());
    }

    @Test
    public void theReplyTopicIsTheSingleCoordinatorTopic() {
        KafkaCallbackSenderImpl sender = new KafkaCallbackSenderImpl();

        Assertions.assertEquals("lra-request", sender.getReplyTopic());
    }
}
