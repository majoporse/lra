/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client;

import io.narayana.lra.connectors.common.callbacks.CallbackAddress;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import java.net.URI;

/**
 * Writes the callback endpoints a participant wants the coordinator to use.
 * <p>
 * A resolver never sends anything; it only produces data for the join request.
 */
public interface ParticipantCallbackAddressResolver {

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

    /**
     * Build one address. Legacy participant methods each have their own path, so the resolver is
     * asked per endpoint instead of for the whole address book.
     *
     * @param type the callback kind
     * @param participantId the participant id
     * @param uri the endpoint URI the participant declared
     * @return the connector's address for that endpoint
     */
    CallbackAddress address(CallbackType type, String participantId, URI uri);
}
