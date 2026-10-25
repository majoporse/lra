/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.contracts;

import io.narayana.lra.api.LRA;

/**
 * The shared shape of a contract reply: the participant's declared outcome and an optional error.
 * <p>
 * The HTTP contract replies inherit {@link io.narayana.lra.connectors.common.contracts.ParticipantReply};
 * the Kafka replies implement {@code KafkaCallbackReply}. Both expose this interface, so the
 * outcome is mapped once without introducing a single neutral reply type.
 */
public interface CallbackOutcome {

    LRA.ParticipantOutcome status();

    String error();
}
