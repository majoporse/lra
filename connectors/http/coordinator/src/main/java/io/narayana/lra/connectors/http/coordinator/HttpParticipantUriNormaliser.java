/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.coordinator;

import io.narayana.lra.connectors.common.coordinator.ParticipantUriNormaliser;
import io.narayana.lra.connectors.http.support.LinkHelper;
import java.net.URISyntaxException;

/**
 * Parses the legacy HTTP participant URLs (plain URIs and link headers) for the coordinator's
 * participant lookup.
 */
public final class HttpParticipantUriNormaliser implements ParticipantUriNormaliser {

    @Override
    public String canonicalRecoveryUri(String participantUrl) throws URISyntaxException {
        return LinkHelper.cannonicalForm(participantUrl);
    }

    @Override
    public String compensatorUri(String participantUrl) throws URISyntaxException {
        return LinkHelper.extractCompensator(participantUrl);
    }
}
