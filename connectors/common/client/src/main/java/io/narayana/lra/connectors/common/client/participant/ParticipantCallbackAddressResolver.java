/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.participant;

import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;

/**
 * Writes the callback address book a participant wants the coordinator to use.
 * <p>
 * A resolver never sends anything; it only produces data for the join request. It owns every
 * protocol detail of the address: whether a callback is served by the participant's own endpoint or
 * by the callback proxy, and the base URI the addresses are built from.
 */
public interface ParticipantCallbackAddressResolver {

    /**
     * Build the callback endpoints for one participant.
     *
     * @param participant the participant whose class and declared callbacks define the addresses
     * @return the address book for the participant
     */
    ParticipantCallbacks resolve(Participant participant);
}
