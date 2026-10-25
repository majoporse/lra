/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client;

import io.narayana.lra.connectors.common.callbacks.CallbackType;
import java.net.URI;
import java.util.Set;

/**
 * The data a connector needs to write a participant's callback endpoints.
 *
 * @param participantId the participant id
 * @param callbackTypes the callback kinds the participant declares
 * @param baseUri the participant's base URI, when the connector needs one (nullable)
 */
public record ParticipantCallbackTarget(
        String participantId,
        Set<CallbackType> callbackTypes,
        URI baseUri) {
}
