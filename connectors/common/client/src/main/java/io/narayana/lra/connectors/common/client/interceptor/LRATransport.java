/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.interceptor;

import io.narayana.lra.connectors.common.client.invocation.LraCompletion;
import io.narayana.lra.connectors.common.client.invocation.LraInvocation;
import jakarta.interceptor.InvocationContext;
import java.net.URI;

/**
 * The transport half of an intercepted invocation: it supplies inbound data and transport
 * mechanics, and never decides LRA state.
 * <p>
 * Each transport supplies an implementation (HTTP reads headers and patches JAX-RS arguments; a
 * messaging transport reads record headers). When none is present the interceptor falls back to
 * {@link CurrentTransport}.
 */
public interface LRATransport {

    /**
     * @return the LRA the caller propagated, or {@code null} when there is none
     * @throws io.narayana.lra.connectors.common.client.LRAAbortException when the inbound value is
     *         malformed and the invocation must be stopped before the business method runs
     */
    URI incomingLRA();

    /**
     * @return the raw credential the caller propagated (for example a JWT), or {@code null}
     */
    String credential();

    /**
     * Publish the effective LRA to the transport and patch any arguments the transport resolved
     * before the interceptor chain ran.
     *
     * @param invocation the interception context
     * @param lraInvocation the state associated with this invocation
     */
    void bind(InvocationContext invocation, LraInvocation lraInvocation);

    /**
     * Interpret the method result (or exception) for the LRA.
     * <p>
     * This is the transport's own mapping. It does not depend on the container's final response
     * mapping: a JAX-RS adapter translates its {@code Response} and exceptions here, and a
     * messaging adapter translates its reply.
     *
     * @param lraInvocation the invocation state
     * @param result the value returned by the business method, or {@code null}
     * @param thrown the exception thrown by the business method, or {@code null}
     * @return the neutral completion
     */
    LraCompletion completionOf(LraInvocation lraInvocation, Object result, Throwable thrown);

    /**
     * @param invocation the interception context
     * @return whether this transport has a response phase that can report lifecycle failures
     */
    default boolean hasResponsePhase(InvocationContext invocation) {
        return false;
    }

    /**
     * Offer the response phase to the transport. A transport that owns completion after a response
     * (HTTP asynchronous responses) stores the invocation and completes it once the response
     * exists.
     *
     * @param invocation the interception context
     * @param lraInvocation the invocation state to complete later
     * @return whether the transport will complete the lifecycle itself
     */
    boolean deferCompletion(InvocationContext invocation, LraInvocation lraInvocation);
}
