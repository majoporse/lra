/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.proxy.callbacks;

import io.narayana.lra.callbacks.KafkaCallback;
import io.narayana.lra.callbacks.ParticipantCallbacks;
import io.narayana.lra.client.internal.proxy.nonjaxrs.LRAParticipant;
import io.narayana.lra.contracts.kafka.LRAKafkaConstants;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Generates Kafka callbacks that are delivered to the participant's client-side listener
 * topic. The target uid is the participant class name so the listener can look the
 * participant up in its registry.
 */
public class KafkaParticipantCallbackGenerator implements ParticipantCallbackGenerator {

    static final String COORDINATOR_TOPIC_PROPERTY = "quarkus.lra.coordinator-topic";

    @Override
    public ParticipantCallbacks getCallbacks(LRAParticipant participant, ParticipantCallbacks existing) {
        String topic = ConfigProvider.getConfig()
                .getOptionalValue(COORDINATOR_TOPIC_PROPERTY, String.class)
                .orElse(LRAKafkaConstants.TOPIC_REQUEST);
        String targetUid = participant.getJavaClass().getName();

        ParticipantCallbacks callbacks = new ParticipantCallbacks();

        if (existing.completeCallback != null) {
            callbacks.completeCallback = existing.completeCallback;
        } else if (participant.hasCompleteMethod()) {
            callbacks.completeCallback = KafkaCallback.completeCallback(topic, targetUid);
        }

        if (existing.compensateCallback != null) {
            callbacks.compensateCallback = existing.compensateCallback;
        } else if (participant.hasCompensateMethod()) {
            callbacks.compensateCallback = KafkaCallback.compensateCallback(topic, targetUid);
        }

        if (existing.statusCallback != null) {
            callbacks.statusCallback = existing.statusCallback;
        } else if (participant.hasStatusMethod()) {
            callbacks.statusCallback = KafkaCallback.statusCallback(topic, targetUid);
        }

        if (existing.forgetCallback != null) {
            callbacks.forgetCallback = existing.forgetCallback;
        } else if (participant.hasForgetMethod()) {
            callbacks.forgetCallback = KafkaCallback.forgetCallback(topic, targetUid);
        }

        if (existing.afterCallback != null) {
            callbacks.afterCallback = existing.afterCallback;
        } else if (participant.hasAfterLRAMethod()) {
            callbacks.afterCallback = KafkaCallback.afterCallback(topic, targetUid);
        }

        return callbacks;
    }
}