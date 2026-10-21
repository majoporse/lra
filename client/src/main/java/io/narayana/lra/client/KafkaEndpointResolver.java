/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

import io.narayana.lra.callbacks.CallbackFactory;
import io.narayana.lra.callbacks.CallbackType;
import io.narayana.lra.callbacks.KafkaCallback;
import io.narayana.lra.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.api.CallbackPaths;
import io.narayana.lra.connectors.api.ParticipantCallbackEndpointResolver;
import io.narayana.lra.connectors.api.ParticipantCallbackTarget;
import io.narayana.lra.connectors.api.ParticipantConnector;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Writes the service-topic endpoints for a Kafka participant.
 */
@ApplicationScoped
@ParticipantConnector(CallbackPaths.KAFKA)
public class KafkaEndpointResolver implements ParticipantCallbackEndpointResolver {

    @Override
    public String protocolId() {
        return CallbackPaths.KAFKA;
    }

    @Override
    public ParticipantCallbacks resolve(ParticipantCallbackTarget target) {
        String topic = ConfigProvider.getConfig()
                .getValue(CallbackFactory.SERVICE_TOPIC_PROPERTY, String.class);
        ParticipantCallbacks callbacks = new ParticipantCallbacks();

        for (CallbackType type : target.callbackTypes()) {
            callbacks.callback(type, KafkaCallback.of(type, topic, target.participantId()));
        }

        return callbacks;
    }
}
