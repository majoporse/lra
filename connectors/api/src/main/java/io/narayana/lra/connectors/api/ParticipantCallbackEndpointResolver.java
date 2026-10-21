/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.api;

import io.narayana.lra.callbacks.ParticipantCallbacks;

/**
 * Writes the callback endpoints a participant wants the coordinator to use.
 * <p>
 * A resolver never sends anything; it only produces data for the join request.
 */
public interface ParticipantCallbackEndpointResolver {

    /**
     * @return the participant protocol id this resolver implements ({@code "http"} or {@code "kafka"})
     */
    String protocolId();

    /**
     * Build the callback endpoints for one participant.
     *
     * @param target the participant id, the callback kinds it declares and its base URI (if the
     *        wire format needs one)
     * @return the address book for the participant
     */
    ParticipantCallbacks resolve(ParticipantCallbackTarget target);
}
