/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

import io.narayana.lra.callbacks.CallbackFactory;
import io.narayana.lra.callbacks.CallbackType;
import io.narayana.lra.callbacks.KafkaCallback;
import io.narayana.lra.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.api.ParticipantCallbackTarget;
import java.net.URI;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KafkaEndpointResolverTest {

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
    public void writesTheServiceTopicEndpoints() {
        ParticipantCallbacks callbacks = new KafkaEndpointResolver().resolve(new ParticipantCallbackTarget(
                "order-service",
                Set.of(CallbackType.COMPENSATE, CallbackType.STATUS),
                URI.create("http://order:8080/")));

        KafkaCallback compensate = (KafkaCallback) callbacks.callback(CallbackType.COMPENSATE);
        KafkaCallback status = (KafkaCallback) callbacks.callback(CallbackType.STATUS);

        Assertions.assertEquals(SERVICE_TOPIC, compensate.getTopic());
        Assertions.assertEquals("order-service", compensate.getParticipantId());
        Assertions.assertEquals(KafkaCallback.Operation.COMPENSATE, compensate.getOperation());
        Assertions.assertEquals(KafkaCallback.Operation.STATUS, status.getOperation());
        Assertions.assertNull(callbacks.callback(CallbackType.FORGET));
    }

    @Test
    public void hasTheKafkaProtocolId() {
        Assertions.assertEquals("kafka", new KafkaEndpointResolver().protocolId());
    }
}
