/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.client;

import io.narayana.lra.connectors.common.callbacks.CallbackAddress;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.client.CallbackPaths;
import io.narayana.lra.connectors.common.client.ParticipantCallbackAddressResolver;
import io.narayana.lra.connectors.common.client.ParticipantCallbackTarget;
import io.narayana.lra.connectors.common.client.ParticipantConnector;
import io.narayana.lra.connectors.common.config.CallbackConfig;
import io.narayana.lra.connectors.kafka.address.KafkaAddress;
import jakarta.enterprise.context.ApplicationScoped;
import java.net.URI;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Writes the service-topic endpoints for a Kafka participant.
 */
@ApplicationScoped
@ParticipantConnector(CallbackPaths.KAFKA)
public class KafkaAddressResolver implements ParticipantCallbackAddressResolver {

    @Override
    public String protocolId() {
        return CallbackPaths.KAFKA;
    }

    @Override
    public ParticipantCallbacks resolve(ParticipantCallbackTarget target) {
        String topic = ConfigProvider.getConfig()
                .getValue(CallbackConfig.SERVICE_TOPIC_PROPERTY, String.class);
        ParticipantCallbacks callbacks = new ParticipantCallbacks();

        for (CallbackType type : target.callbackTypes()) {
            callbacks.callback(type, new KafkaAddress(type, target.participantId(), topic));
        }

        return callbacks;
    }

    @Override
    public CallbackAddress address(CallbackType type, String participantId, URI uri) {
        String topic = ConfigProvider.getConfig()
                .getValue(CallbackConfig.SERVICE_TOPIC_PROPERTY, String.class);

        return new KafkaAddress(type, participantId, topic);
    }
}
