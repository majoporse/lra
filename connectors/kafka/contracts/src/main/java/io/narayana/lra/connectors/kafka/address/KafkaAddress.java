/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.address;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.narayana.lra.connectors.common.callbacks.CallbackAddress;
import io.narayana.lra.connectors.common.callbacks.CallbackType;

/**
 * A callback delivered on the participant's Kafka service topic.
 */
@JsonTypeName("kafka")
public record KafkaAddress(
        CallbackType operation,
        String participantId,
        String topic) implements CallbackAddress {

    @Override
    public CallbackAddress withOperation(CallbackType operation) {
        return new KafkaAddress(operation, participantId, topic);
    }
}
