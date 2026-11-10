/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.lifecycle;

import io.narayana.lra.api.Outcome;
import io.narayana.lra.connectors.common.client.invocation.LraInvocation;

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
     * @param context the state associated with this invocation
     */
    void processRequest(LraInvocation context);

    /**
     * Run the response phase and report what the caller has to be told.
     *
     * @param context the state associated with this invocation
     * @param cancel whether the invocation result matches the cancel conditions
     * @return the decision for this invocation
     */
    LifecycleDecision processResponse(LraInvocation context, boolean cancel);

    /**
     * Decide whether a transport-neutral outcome matches the cancel conditions of a plain CDI
     * invocation, where there is no status code.
     *
     * @param context the state associated with this invocation
     * @param outcome the outcome carried by the method result (or implied by an exception)
     * @param failed whether the method failed
     * @return whether the LRA must be cancelled
     */
    boolean isCancel(LraInvocation context, Outcome outcome, boolean failed);

    /**
     * Decide whether a status reported by the transport matches the cancel conditions.
     *
     * @param context the state associated with this invocation
     * @param statusCode the transport status
     * @return whether the LRA must be cancelled
     */
    boolean isCancel(LraInvocation context, int statusCode);
}
