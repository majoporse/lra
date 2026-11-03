/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.invocation;

import io.narayana.lra.Current;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * A temporary association between an LRA and the current thread that can be undone without
 * disturbing the context of an enclosing invocation.
 * <p>
 * Activation registers the LRA in the active cache as well as pushing it: {@link Current#peek()}
 * discards a context whose LRA is not cached, so a bare push would be invisible.
 */
public final class CurrentScope {

    private final List<Object> contextsOnEntry;
    private final String authTokenOnEntry;
    private final Deque<URI> activated = new ArrayDeque<>();

    private CurrentScope(List<Object> contextsOnEntry, String authTokenOnEntry) {
        this.contextsOnEntry = contextsOnEntry;
        this.authTokenOnEntry = authTokenOnEntry;
    }

    /**
     * Snapshot the thread-local LRA stack and auth token.
     */
    public static CurrentScope capture() {
        return new CurrentScope(Current.getContexts(), Current.getAuthToken());
    }

    /**
     * Associate the LRA with the current thread until {@link #restore()}. A {@code null} LRA is a
     * no-op.
     */
    public void activate(URI lraId) {
        if (lraId == null) {
            return;
        }

        Current.push(lraId, null);
        Current.addActiveLRACache(lraId);
        activated.push(lraId);
    }

    /**
     * Undo everything this scope did, then re-establish the state captured on entry.
     */
    public void restore() {
        while (!activated.isEmpty()) {
            Current.removeActiveLRACache(activated.pop());
        }

        restore(contextsOnEntry, authTokenOnEntry);
    }

    /**
     * Restore a previously captured stack and auth token: pop the contexts added since, re-push any
     * that were removed, and replace the token.
     */
    static void restore(List<Object> contextsOnEntry, String authTokenOnEntry) {
        List<Object> contexts = Current.getContexts();

        while (contexts.size() > contextsOnEntry.size()) {
            Current.pop();
            contexts = Current.getContexts();
        }

        for (Object contextOnEntry : contextsOnEntry) {
            if (!contexts.contains(contextOnEntry)) {
                Current.push((URI) contextOnEntry, null);
                contexts = Current.getContexts();
            }
        }

        if (authTokenOnEntry == null) {
            Current.clearAuthToken();
        } else {
            Current.setAuthToken(authTokenOnEntry);
        }
    }
}
