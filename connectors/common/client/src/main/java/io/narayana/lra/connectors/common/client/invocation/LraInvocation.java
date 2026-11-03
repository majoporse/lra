/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.invocation;

import io.narayana.lra.Current;
import java.util.List;

/**
 * The state the interceptor hands to the transport response adapter.
 * <p>
 * The interceptor runs before the response exists, so it cannot close or cancel the LRA itself
 * when the transport is HTTP: the cancel rules are expressed in HTTP status codes. This object is
 * stored as a request property and read by the thin JAX-RS response filter, which completes the
 * lifecycle once the final status is known.
 */
public final class LraInvocation {

    /**
     * Request property under which the interceptor stores the invocation state.
     */
    public static final String PROPERTY = LraInvocation.class.getName();

    private final ServerLRAContext context;
    private final List<Object> contextsOnEntry;
    private final String authTokenOnEntry;

    public LraInvocation(ServerLRAContext context, List<Object> contextsOnEntry, String authTokenOnEntry) {
        this.context = context;
        this.contextsOnEntry = contextsOnEntry;
        this.authTokenOnEntry = authTokenOnEntry;
    }

    public ServerLRAContext getContext() {
        return context;
    }

    /**
     * Restore the thread-local state that was in effect before the intercepted invocation.
     * <p>
     * The interceptor can be nested inside other LRA processing, so it only undoes the changes made
     * by its own request phase (and re-establishes an incoming LRA that the processor suspended or
     * removed).
     */
    public void restoreCurrent() {
        if (context.getCurrentLRA() != null) {
            Current.removeActiveLRACache(context.getCurrentLRA());
        }

        CurrentScope.restore(contextsOnEntry, authTokenOnEntry);
    }
}
