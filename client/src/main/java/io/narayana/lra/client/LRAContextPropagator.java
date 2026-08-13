/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

import java.net.URI;

/**
 * Reads and writes LRA context from/to transport-specific messages.
 * Implementations: {@code LRAContextPropagatorHTTP}, {@code LRAContextPropagatorMessaging}.
 *
 * <p>
 * The {@code message} parameter is intentionally untyped ({@code Object}) —
 * each implementation knows what type to expect (e.g., {@code ContainerRequestContext}
 * for HTTP, {@code Message<String>} for Kafka).
 * </p>
 */
public interface LRAContextPropagator {

    /**
     * Read the active LRA context from an incoming message.
     * For HTTP: reads {@code Long-Running-Action} header.
     * For messaging: reads {@code lraId} from message body envelope.
     *
     * @param message the incoming message
     * @return the LRA URI, or null if not present
     */
    URI readIncomingContext(Object message);

    /**
     * Write LRA context to an outgoing message.
     * For HTTP: sets {@code Long-Running-Action} header.
     * For messaging: wraps payload in envelope with {@code lraId}.
     *
     * @param message the outgoing message
     * @param lraId the LRA URI to propagate
     */
    void writeOutgoingContext(Object message, URI lraId);

    /**
     * Clear LRA context from a message.
     */
    void clearContext(Object message);
}
