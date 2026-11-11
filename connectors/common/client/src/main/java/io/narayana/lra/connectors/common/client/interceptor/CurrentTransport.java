/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.interceptor;

import io.narayana.lra.Current;
import io.narayana.lra.api.Outcome;
import io.narayana.lra.api.Result;
import io.narayana.lra.connectors.common.client.invocation.LraCompletion;
import io.narayana.lra.connectors.common.client.invocation.LraInvocationState;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.interceptor.InvocationContext;
import java.net.URI;

/**
 * The transport used when no connector supplies one: the inbound LRA is the effective one on the
 * calling thread and the transport neither binds anything nor completes the lifecycle itself.
 * <p>
 * A connector's transport is an {@code @Alternative} with a priority, so it replaces this bean when
 * the connector is on the classpath.
 */
@ApplicationScoped
public class CurrentTransport implements LRATransport {

    @Override
    public URI incomingLRA() {
        return Current.peek();
    }

    @Override
    public String credential() {
        return null;
    }

    @Override
    public void bind(InvocationContext invocation, LraInvocationState state) {
        // nothing is resolved by the transport
    }

    @Override
    public LraCompletion completionOf(LraInvocationState state, Object result, Throwable thrown) {
        if (state.isAborted()) {
            return LraCompletion.failure(state.getAbortMessage());
        }

        if (thrown != null) {
            return LraCompletion.failure(thrown.getMessage());
        }

        if (result instanceof Result<?> wrapper) {
            Outcome outcome = wrapper.getOutcome();
            return new LraCompletion(outcome == null ? Outcome.SUCCESS : outcome, null, null, false);
        }

        return LraCompletion.success();
    }
}
