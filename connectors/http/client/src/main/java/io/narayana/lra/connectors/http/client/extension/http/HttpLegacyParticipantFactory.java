/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.extension.http;

import io.narayana.lra.connectors.common.client.extension.spi.LegacyParticipantFactory;
import io.narayana.lra.connectors.common.client.participant.Participant;
import io.narayana.lra.connectors.http.client.legacy.LegacyLRAParticipant;

/**
 * Contributes the legacy {@code @Path} participant model to the connector-neutral discovery loop.
 * <p>
 * {@link LegacyLRAParticipant} is the JAX-RS-bound model (it scans {@code @Path} verbs and its
 * callbacks may return {@code Response}); the discovery itself lives in
 * {@code extension.io.narayana.lra.connectors.common.client.extension.LRACDIExtension}.
 */
public final class HttpLegacyParticipantFactory implements LegacyParticipantFactory {

    @Override
    public Participant create(Class<?> javaClass) {
        return new LegacyLRAParticipant(javaClass);
    }
}
