/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

import java.net.URI;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

/**
 * Abstract interface for communicating with the LRA coordinator.
 * Implementations: {@code CoordinatorClientHTTP} (REST), {@code CoordinatorClientMessaging} (Kafka).
 *
 * <p>
 * This interface is transport-agnostic — no JAX-RS annotations.
 * The implementation is selected by the {@code lra.transport.type} configuration property.
 * </p>
 */
public interface CoordinatorClient extends AutoCloseable {

    /**
     * Start a new LRA.
     *
     * @param clientId client identifier
     * @param timeLimit time limit in milliseconds
     * @param parentLRA parent LRA identifier (for nested LRAs), or null
     * @return the URI of the new LRA
     */
    URI startLRA(String clientId, long timeLimit, URI parentLRA) throws LRAException;

    /**
     * Join an existing LRA (register a participant).
     *
     * @param lraId the LRA to join
     * @param timeLimit time limit for participant compensation
     * @param registrationData callback registration data (Link header for HTTP, JSON body for messaging)
     * @param compensatorData participant-specific data
     * @return the recovery URI
     */
    URI joinLRA(URI lraId, long timeLimit, String registrationData, String compensatorData) throws LRAException;

    /**
     * Close (complete) an LRA.
     */
    void closeLRA(URI lraId, String compensator, String userData) throws LRAException;

    /**
     * Cancel (compensate) an LRA.
     */
    void cancelLRA(URI lraId, String compensator, String userData) throws LRAException;

    /**
     * Get the current status of an LRA.
     */
    LRAStatus getLRAStatus(URI lraId) throws LRAException;

    /**
     * Remove a participant from an LRA.
     */
    void leaveLRA(URI lraId, String compensatorUrl) throws LRAException;

    /**
     * Renew the time limit for an existing LRA.
     */
    void renewTimeLimit(URI lraId, long timeLimit) throws LRAException;

    /**
     * Get the status of a nested LRA.
     */
    LRAStatus getNestedLRAStatus(URI nestedLraId) throws LRAException;

    /**
     * Complete a nested LRA.
     */
    void completeNestedLRA(URI nestedLraId) throws LRAException;

    /**
     * Compensate a nested LRA.
     */
    void compensateNestedLRA(URI nestedLraId) throws LRAException;

    /**
     * Forget a nested LRA.
     */
    void forgetNestedLRA(URI nestedLraId) throws LRAException;

    @Override
    void close();
}
