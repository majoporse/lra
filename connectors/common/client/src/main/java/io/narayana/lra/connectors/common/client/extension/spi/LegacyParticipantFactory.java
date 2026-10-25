/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.extension.spi;

import io.narayana.lra.connectors.common.client.participant.Participant;

/**
 * Builds the legacy participant model for the classes the shared discovery loop finds.
 * <p>
 * Discovery itself is connector-neutral and lives in {@code io.narayana.lra.connectors.common.client.extension}.
 * The legacy participant model is JAX-RS-bound ({@code @Path} methods, {@code Response} results),
 * so its construction stays in the HTTP connector and is contributed through this service.
 */
public interface LegacyParticipantFactory {

    /**
     * @param javaClass a class the discovery loop has selected
     * @return the participant model
     */
    Participant create(Class<?> javaClass);
}
