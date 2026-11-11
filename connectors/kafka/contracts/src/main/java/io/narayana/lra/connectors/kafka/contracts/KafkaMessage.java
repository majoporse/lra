/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.contracts;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A Kafka message that carries its wire type and can be correlated with its reply.
 */
public interface KafkaMessage {

    String getCorrelationId();

    /**
     * The envelope type. It is not part of the payload JSON.
     */
    @JsonIgnore
    String getType();
}
