/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

import java.util.Map;

/**
 * Builds callback registration data for a participant.
 * Implementations: {@code CallbackRegistrarHTTP} (Link headers), {@code CallbackRegistrarMessaging} (JSON body).
 */
public interface CallbackRegistrar {

    /**
     * Scan a participant class for callback annotations and build registration data.
     *
     * @param compensatorClass the participant class
     * @param uriPrefix the base URI for callbacks
     * @param timeout the time limit
     * @return registration data (Link header string for HTTP, JSON body for messaging)
     */
    String buildRegistration(Class<?> compensatorClass, String uriPrefix, Long timeout);

    /**
     * Get the termination URIs for a participant class.
     * Returns a map of rel to URI (e.g., "compensate" to "http://..." or "kafka://...").
     *
     * @param compensatorClass the participant class
     * @param uriPrefix the base URI for callbacks
     * @param timeout the time limit
     * @return map of rel to URI
     */
    Map<String, String> getTerminationUris(Class<?> compensatorClass, String uriPrefix, Long timeout);
}
