/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.domain.model;

import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

/**
 * Abstraction for delivering LRA participant callbacks.
 * Implementations: {@link HttpParticipantNotifier} (direct HTTP),
 * {@link MessagingParticipantNotifier} (sends to message topic).
 *
 * <p>
 * The coordinator uses {@link ParticipantNotifierHolder#getForEndpoint(String)}
 * to select the appropriate notifier based on the callback endpoint scheme.
 * </p>
 */
public interface ParticipantNotifier {

    /**
     * Check if this notifier supports the given endpoint scheme.
     *
     * @param endpoint the callback endpoint string (e.g., "http://..." or "kafka://...")
     * @return true if this notifier can deliver to the endpoint
     */
    boolean supports(String endpoint);

    /**
     * Deliver a compensate callback.
     *
     * @param lraId the LRA id
     * @param parentId the parent LRA id (may be null)
     * @param targetEndpoint the callback endpoint (http:// or kafka://)
     * @param recoveryURI the recovery URI
     * @param compensatorData participant-specific data
     * @return HTTP status code (for HTTP notifier) or -1 for async delivery (messaging notifier)
     */
    int notifyCompensate(String lraId, String parentId, String targetEndpoint,
            String recoveryURI, String compensatorData);

    /**
     * Deliver a complete callback.
     *
     * @return HTTP status code (for HTTP notifier) or -1 for async delivery (messaging notifier)
     */
    int notifyComplete(String lraId, String parentId, String targetEndpoint,
            String recoveryURI, String compensatorData);

    /**
     * Deliver a forget callback.
     *
     * @return true if delivered successfully
     */
    boolean notifyForget(String lraId, String parentId, String targetEndpoint,
            String recoveryURI, String compensatorData);

    /**
     * Deliver an afterLRA callback.
     *
     * @param payload the LRAStatus name (e.g., "Closed", "Cancelled")
     * @return true if delivered successfully
     */
    boolean notifyAfterLRA(String lraId, String parentId, String targetEndpoint,
            String recoveryURI, String compensatorData, String payload);

    /**
     * Query participant status (synchronous). Only meaningful for HTTP notifier.
     * Messaging notifier returns null (status queries stay HTTP-only).
     *
     * @return participant status, or null if not applicable
     */
    ParticipantStatus queryStatus(String lraId, String statusEndpoint,
            String recoveryURI, String compensatorData);
}
