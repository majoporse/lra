/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.coordinator;

import io.narayana.lra.api.LRA;
import io.narayana.lra.connectors.common.callbacks.CallbackContext;
import io.narayana.lra.connectors.common.callbacks.CallbackResult;
import io.narayana.lra.connectors.common.callbacks.CallbackStatus;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.kafka.address.KafkaAddress;
import io.narayana.lra.connectors.kafka.contracts.CompensateKafkaCallback;
import io.narayana.lra.connectors.kafka.contracts.KafkaCallbackReply;
import io.narayana.lra.connectors.kafka.contracts.KafkaReply;
import io.narayana.lra.connectors.kafka.contracts.KafkaRequest;
import io.narayana.lra.connectors.kafka.outbound.KafkaOutbound;
import io.narayana.lra.connectors.kafka.support.LRAKafkaConstants;
import java.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KafkaDeliveryTest {

    @Test
    public void sendsTheCallbackKindAndMapsTheReply() {
        CapturingOutbound outbound = new CapturingOutbound();
        outbound.reply = new CompensateKafkaCallback.Reply("correlation",
                LRA.ParticipantOutcome.Compensated, null);
        KafkaDelivery delivery = delivery(outbound);

        CallbackResult result = delivery.request(
                new KafkaAddress(CallbackType.COMPENSATE, "participant", "service-topic"),
                new CallbackContext("http://lra/1", null, "http://recovery/1", "data"),
                Duration.ofSeconds(1));

        Assertions.assertEquals("service-topic", outbound.topic);
        Assertions.assertEquals(LRAKafkaConstants.TYPE_COMPENSATE, outbound.type);
        Assertions.assertEquals("coordinator-topic", outbound.request.getReplyTopic());
        Assertions.assertEquals("participant",
                ((CompensateKafkaCallback.Request) outbound.request).participantId);
        Assertions.assertNotNull(outbound.correlationId);
        Assertions.assertEquals(CallbackStatus.OK, result.status);
    }

    @Test
    public void returnsTimeoutWhenNoReplyArrives() {
        CapturingOutbound outbound = new CapturingOutbound();
        KafkaDelivery delivery = delivery(outbound);

        CallbackResult result = delivery.request(
                new KafkaAddress(CallbackType.COMPENSATE, "participant", "service-topic"),
                new CallbackContext("http://lra/1", null, null, null),
                Duration.ofSeconds(1));

        Assertions.assertEquals(CallbackStatus.TIMEOUT, result.status);
    }

    @Test
    public void returnsErrorWhenTheWireFails() {
        CapturingOutbound outbound = new CapturingOutbound();
        outbound.failure = new IllegalStateException("broker down");
        KafkaDelivery delivery = delivery(outbound);

        CallbackResult result = delivery.request(
                new KafkaAddress(CallbackType.COMPENSATE, "participant", "service-topic"),
                new CallbackContext("http://lra/1", null, null, null),
                Duration.ofSeconds(1));

        Assertions.assertEquals(CallbackStatus.ERROR, result.status);
        Assertions.assertEquals("broker down", result.body);
    }

    private static KafkaDelivery delivery(KafkaOutbound outbound) {
        KafkaDelivery delivery = new KafkaDelivery();
        delivery.kafkaOutbound = outbound;
        return delivery;
    }

    private static final class CapturingOutbound extends KafkaOutbound {
        private String topic;
        private String type;
        private String correlationId;
        private KafkaRequest<?> request;
        private KafkaCallbackReply reply;
        private RuntimeException failure;

        @Override
        public <R extends KafkaReply> R request(String topic, KafkaRequest<R> request, Duration timeout) {
            if (failure != null) {
                throw failure;
            }

            this.topic = topic;
            this.type = request.getType();
            this.correlationId = request.getCorrelationId();
            this.request = request;
            return request.getReplyType().cast(reply);
        }

        @Override
        public String coordinatorTopic() {
            return "coordinator-topic";
        }
    }
}
