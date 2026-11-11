/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.participant;

import io.narayana.lra.connectors.common.client.discovery.MicroProfileParticipantFactory;
import io.narayana.lra.connectors.common.client.participant.Participant;

/**
 * Contributes the legacy {@code @Path} participant model to the connector-neutral discovery loop.
 * <p>
 * {@link MicroProfileLRAParticipant} is the JAX-RS-bound model (it scans {@code @Path} verbs and its
 * callbacks may return {@code Response}); the discovery itself lives in
 * {@code io.narayana.lra.connectors.common.client.discovery.PortableParticipantDiscovery}.
 */
public final class HttpMicroProfileParticipantFactory implements MicroProfileParticipantFactory {

    @Override
    public Participant create(Class<?> javaClass) {
        return new MicroProfileLRAParticipant(javaClass);
    }
}
