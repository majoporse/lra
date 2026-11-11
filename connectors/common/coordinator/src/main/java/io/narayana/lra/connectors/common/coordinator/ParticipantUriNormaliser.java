/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.coordinator;

import java.net.URISyntaxException;

/**
 * Turns a participant URL as it arrived at the coordinator into the URI forms the domain matches
 * its participant records with.
 * <p>
 * The legacy HTTP protocol allowed a participant to send a link header instead of a plain URI, so
 * the parsing belongs to the HTTP connector. The domain asks this seam and never parses HTTP wire
 * syntax itself. Without a registered normaliser the participant URL is used as it is.
 */
public interface ParticipantUriNormaliser {

    /**
     * @return the URI to match the participant's recovery URL against
     */
    String canonicalRecoveryUri(String participantUrl) throws URISyntaxException;

    /**
     * @return the participant's compensate URI, extracted from a legacy link header
     */
    String compensatorUri(String participantUrl) throws URISyntaxException;
}
