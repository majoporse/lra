/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.lifecycle;

import io.narayana.lra.api.Outcome;
import io.narayana.lra.connectors.common.client.invocation.LraCompletion;
import io.narayana.lra.connectors.common.client.invocation.LraInvocationState;

/**
 * The one lifecycle state machine. It turns the resolved transaction metadata into start, join,
 * enlist, end, leave, cancel and close operations, and reports the resulting decision.
 * <p>
 * Connectors contribute their coordinator client, participant model and callback address resolver;
 * they never implement their own processor.
 */
public interface LraLifecycleProcessor {

    /**
     * Run the request phase: apply {@code @PropagateToken}, {@code @Leave} and the propagation type,
     * enlisting the participant when the invocation joins an LRA.
     *
     * @param invocation the state associated with this invocation
     */
    void processRequest(LraInvocationState invocation);

    /**
     * Run the response phase and report what the caller has to be told.
     *
     * @param invocation the state associated with this invocation
     * @param cancel whether the invocation result matches the cancel conditions
     * @return the decision for this invocation
     */
    LifecycleDecision processResponse(LraInvocationState invocation, boolean cancel);

    /**
     * Decide whether a transport-neutral outcome matches the cancel conditions of a plain CDI
     * invocation, where there is no status code.
     *
     * @param invocation the state associated with this invocation
     * @param outcome the outcome carried by the method result (or implied by an exception)
     * @param failed whether the method failed
     * @return whether the LRA must be cancelled
     */
    boolean isCancel(LraInvocationState invocation, Outcome outcome, boolean failed);

    /**
     * Decide whether a status reported by the transport matches the cancel conditions.
     *
     * @param invocation the state associated with this invocation
     * @param statusCode the transport status
     * @return whether the LRA must be cancelled
     */
    boolean isCancel(LraInvocationState invocation, int statusCode);

    /**
     * Run the response phase from a transport-neutral completion. This is the one completion entry
     * point used by the interceptor and by a transport response adapter.
     *
     * @param invocation the invocation being completed
     * @param completion the neutral completion produced by the transport
     * @return the decision for this invocation
     */
    default LifecycleDecision complete(LraInvocationState invocation, LraCompletion completion) {
        boolean cancel = completion.statusCode() != null
                ? isCancel(invocation, completion.statusCode())
                : isCancel(invocation, completion.outcome(), completion.failed());

        return processResponse(invocation, cancel);
    }
}
