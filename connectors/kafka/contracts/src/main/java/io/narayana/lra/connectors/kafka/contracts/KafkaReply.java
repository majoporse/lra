/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.contracts;

/**
 * A reply to a {@link KafkaRequest}.
 */
public interface KafkaReply extends KafkaMessage {

    String getError();
}
