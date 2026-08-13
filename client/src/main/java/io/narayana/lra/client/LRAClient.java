/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

import io.narayana.lra.LRAData;
import java.net.URI;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

/**
 * High-level abstraction for managing Long Running Actions (LRAs).
 * This interface is transport-agnostic — implementations may use HTTP or messaging (Kafka).
 *
 * <p>
 * Use {@link LRAClientFactory} to obtain an instance.
 * </p>
 */
public interface LRAClient {

    /**
     * Start a new top-level LRA.
     *
     * @param clientID client identifier
     * @return the URI of the new LRA
     */
    URI startLRA(String clientID);

    /**
     * Start a new LRA, optionally nested under a parent.
     *
     * @param parentLRA parent LRA URI, or null for top-level
     * @param clientID client identifier
     * @param timeout timeout value
     * @param unit timeout unit
     * @return the URI of the new LRA
     */
    URI startLRA(URI parentLRA, String clientID, Long timeout, ChronoUnit unit);

    /**
     * Start a new LRA, optionally nested under a parent.
     *
     * @param parentLRA parent LRA URI, or null for top-level
     * @param clientID client identifier
     * @param timeout timeout value
     * @param unit timeout unit
     * @param verbose whether to log errors
     * @return the URI of the new LRA
     */
    URI startLRA(URI parentLRA, String clientID, Long timeout, ChronoUnit unit, boolean verbose);

    /**
     * Cancel (compensate) an LRA.
     */
    void cancelLRA(URI lraId);

    /**
     * Cancel (compensate) an LRA with compensator and user data.
     */
    void cancelLRA(URI lraId, String compensator, String userData);

    /**
     * Close (complete) an LRA.
     */
    void closeLRA(URI lraId);

    /**
     * Close (complete) an LRA with compensator and user data.
     */
    void closeLRA(URI lraId, String compensator, String userData);

    /**
     * Join an existing LRA as a participant.
     */
    URI joinLRA(URI lraId, Long timeLimit,
            URI compensateUri, URI completeUri, URI forgetUri, URI leaveUri, URI afterUri, URI statusUri,
            String compensatorData);

    URI joinLRA(URI lraId, Long timeLimit,
            URI compensateUri, URI completeUri, URI forgetUri, URI leaveUri, URI afterUri, URI statusUri,
            StringBuilder compensatorData);

    URI joinLRA(URI lraId, Long timeLimit,
            URI participantUri, StringBuilder compensatorData);

    /**
     * Enlist a compensator with a link header.
     */
    URI enlistCompensator(URI uri, Long timelimit, String linkHeader, StringBuilder compensatorData);

    /**
     * Leave an LRA.
     */
    void leaveLRA(URI lraId, String body);

    /**
     * Get the status of an LRA.
     */
    LRAStatus getStatus(URI uri);

    /**
     * Get detailed information about an LRA.
     */
    LRAData getLRAInfo(URI uri);

    /**
     * Get detailed information about an LRA with a specific media type.
     *
     * @param uri The LRA URI
     * @param acceptMediaType Response content type preference
     * @return LRAData containing detailed information about the LRA
     */
    LRAData getLRAInfo(URI uri, String acceptMediaType);

    /**
     * Renew the time limit for an LRA.
     */
    void renewTimeLimit(URI uri, Long timeLimit);

    /**
     * Get the status of a nested LRA.
     */
    ParticipantStatus getNestedLRAStatus(URI nestedLraId);

    /**
     * Complete a nested LRA.
     */
    ParticipantStatus completeNestedLRA(URI nestedLraId);

    /**
     * Compensate a nested LRA.
     */
    ParticipantStatus compensateNestedLRA(URI nestedLraId);

    /**
     * Forget a nested LRA.
     */
    void forgetNestedLRA(URI nestedLraId);

    /**
     * Get all LRAs from the coordinator.
     */
    List<LRAData> getAllLRAs();

    /**
     * Set the current LRA context.
     */
    void setCurrentLRA(URI lraId);

    /**
     * Get the current LRA from the context stack.
     */
    URI getCurrent();

    /**
     * Clear the current LRA context.
     *
     * @param all true to clear all contexts, false to pop one
     */
    void clearCurrent(boolean all);

    /**
     * Get the coordinator URL.
     */
    String getCoordinatorUrl();

    /**
     * Get the recovery coordinator URL.
     */
    String getRecoveryUrl();

    /**
     * Shutdown any started resources.
     */
    void close();
}
