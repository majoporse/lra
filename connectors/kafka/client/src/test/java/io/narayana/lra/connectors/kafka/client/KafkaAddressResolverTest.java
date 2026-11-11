/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.client;

import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.client.participant.LRAParticipant;
import io.narayana.lra.connectors.common.config.CallbackConfig;
import io.narayana.lra.connectors.common.contracts.CompensateCallback;
import io.narayana.lra.connectors.common.contracts.StatusCallback;
import io.narayana.lra.connectors.kafka.address.KafkaAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KafkaAddressResolverTest {

    private static final String SERVICE_TOPIC = "service-topic";

    private String previousTopic;

    @BeforeEach
    public void setServiceTopic() {
        previousTopic = System.getProperty(CallbackConfig.SERVICE_TOPIC_PROPERTY);
        System.setProperty(CallbackConfig.SERVICE_TOPIC_PROPERTY, SERVICE_TOPIC);
    }

    @AfterEach
    public void restoreServiceTopic() {
        if (previousTopic == null) {
            System.clearProperty(CallbackConfig.SERVICE_TOPIC_PROPERTY);
        } else {
            System.setProperty(CallbackConfig.SERVICE_TOPIC_PROPERTY, previousTopic);
        }
    }

    @Test
    public void writesTheServiceTopicAddresses() {
        ParticipantCallbacks callbacks = new KafkaAddressResolver().resolve(new LRAParticipant(KafkaParticipant.class));

        KafkaAddress compensate = (KafkaAddress) callbacks.callback(CallbackType.COMPENSATE);
        KafkaAddress status = (KafkaAddress) callbacks.callback(CallbackType.STATUS);

        Assertions.assertEquals(SERVICE_TOPIC, compensate.topic());
        Assertions.assertEquals(KafkaParticipant.class.getName(), compensate.participantId());
        Assertions.assertEquals(CallbackType.COMPENSATE, compensate.operation());
        Assertions.assertEquals(CallbackType.STATUS, status.operation());
        Assertions.assertNull(callbacks.callback(CallbackType.FORGET));
    }

    public static class KafkaParticipant {
        @org.eclipse.microprofile.lra.annotation.Compensate
        public CompensateCallback.Reply compensate(CompensateCallback.Request request) {
            return null;
        }

        @org.eclipse.microprofile.lra.annotation.Status
        public StatusCallback.Reply status(StatusCallback.Request request) {
            return null;
        }
    }
}
