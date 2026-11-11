/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.contracts;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A request envelope: a wire type, a correlation id, a reply topic and the concrete reply type.
 *
 * @param <R> the reply type
 */
public interface KafkaRequest<R extends KafkaReply> extends KafkaMessage {

    String getReplyTopic();

    /**
     * The concrete reply type. It is correlation metadata, not part of the payload JSON.
     */
    @JsonIgnore
    Class<R> getReplyType();
}
