/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.client;

import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.client.participant.Participant;
import io.narayana.lra.connectors.common.client.participant.ParticipantCallbackAddressResolver;
import io.narayana.lra.connectors.common.client.participant.protocol.CallbackPaths;
import io.narayana.lra.connectors.common.client.participant.protocol.ParticipantConnector;
import io.narayana.lra.connectors.common.config.CallbackConfig;
import io.narayana.lra.connectors.kafka.address.KafkaAddress;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Writes the service-topic endpoints for a Kafka participant.
 */
@ApplicationScoped
@ParticipantConnector(CallbackPaths.KAFKA)
public class KafkaAddressResolver implements ParticipantCallbackAddressResolver {

    @Override
    public ParticipantCallbacks resolve(Participant participant) {
        String topic = ConfigProvider.getConfig()
                .getValue(CallbackConfig.SERVICE_TOPIC_PROPERTY, String.class);
        ParticipantCallbacks callbacks = new ParticipantCallbacks();

        for (CallbackType type : CallbackType.values()) {
            if (participant.hasCallback(type)) {
                callbacks.callback(type, new KafkaAddress(type, participant.participantId(), topic));
            }
        }

        return callbacks;
    }
}
