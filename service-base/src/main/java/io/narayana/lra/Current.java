/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;

import jakarta.ws.rs.container.ContainerResponseContext;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

// similar to ThreadActionData except it need to be available on the client side
// for use by NarayanaLRAClient and ServerLRAFilter
public class Current {
    private static final Config CONFIG = ConfigProvider.getConfig();

    private record LRAContext(
            UUID lraId,
            UUID parentLRA) {
    }

    private static final ThreadLocal<Current> lraContexts = new ThreadLocal<>();

    /**
     * Use this cache to prevent from {@link ThreadLocal} spilling. This cache is incremented when the request filter
     * method is invoked and decremented when the response filter method is invoked. In this sense, if the response
     * filter method runs on different thread (meaning it doesn't clear the right {@link ThreadLocal}) we still remove
     * it from the cache.
     */
    private static final Map<UUID, Integer> activeLRACache = new ConcurrentHashMap<>();

    private static final ThreadLocal<String> authToken = new ThreadLocal<>();

    public static void setAuthToken(String token) {
        authToken.set(token);
    }

    public static String getAuthToken() {
        return authToken.get();
    }

    public static void clearAuthToken() {
        authToken.remove();
    }

    @SuppressWarnings("ConstantConditions")
    public static void addActiveLRACache(UUID lraId) {
        if (lraId == null) {
            return;
        }

        activeLRACache.merge(lraId, 1, Integer::sum);
    }

    public static void removeActiveLRACache(UUID lraId) {
        if (lraId == null) {
            return;
        }

        activeLRACache.compute(lraId, (k, v) -> {
            if (v == null) {
                return null; // already absent; nothing to do
            }

            int next = v - 1;
            return next <= 0 ? null : next; // remove at 0
        });
    }

    private final Stack<LRAContext> stack;

    private Current(UUID lraId, UUID parentLRA) {
        stack = new Stack<>();
        stack.push(new LRAContext(lraId, parentLRA));
    }

    // given an LRA id extract the immediate parent context from the stack
    public static UUID getFirstParent(UUID lraId) {
        if (lraId == null) {
            return null;
        }
        LRAContext context = findContext(lraContexts.get(), lraId);

        return context == null ? null : context.parentLRA();
    }

    // find a context on the current thread's stack that matches the lra id
    private static LRAContext findContext(Current current, UUID lraId) {
        if (current == null || lraId == null) {
            return null;
        }

        for (LRAContext context : current.stack) {
            if (lraId.equals(context.lraId())) {
                return context;
            }
        }

        return null;
    }

    private static void clearContext(Current current) {
        lraContexts.set(null);
    }

    public static UUID peek() {
        Current current = lraContexts.get();
        LRAContext context = current != null && !current.stack.isEmpty() ? current.stack.peek() : null;
        UUID lraId = context == null ? null : context.lraId();

        if (lraId != null && !activeLRACache.containsKey(lraId)) {
            // we cleaned the Current on different thread, so we need to clear the context
            // that was set by previous request filter and wasn't cleaned by the response filter
            Current.popAll();
            return null;
        }

        return lraId;
    }

    // form the http URI of an LRA from its uid; the coordinator url is taken from the
    // context that pushed the LRA on to the current thread
    // the coordinator url used to form LRA ids, derived from configuration
    public static String getCoordinatorUrl() {
        String base = CONFIG.getOptionalValue("lra.coordinator.url", String.class)
                .orElse("http://localhost:8080/" + LRAConstants.COORDINATOR_PATH_NAME);
        int comma = base.indexOf(',');
        if (comma != -1) {
            base = base.substring(0, comma);
        }
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return base;
    }

    // form the http URI of an LRA from its uid
    public static URI toURI(UUID lraId) {
        return lraId == null ? null : URI.create(getCoordinatorUrl() + "/" + lraId);
    }

    // the http URI of the current LRA on the calling thread (used when propagating the context)
    public static URI peekURI() {
        return toURI(peek());
    }

    public static UUID pop() {
        Current current = lraContexts.get();
        UUID lraId = null;

        if (current != null && !current.stack.isEmpty()) {
            lraId = current.stack.pop().lraId(); // there must be at least one

            if (current.stack.empty()) {
                clearContext(current);
            }
        }

        return lraId;
    }

    // dissassociate an LRA from the callers thread (including any child LRAs)
    public static boolean pop(UUID lra) {
        Current current = lraContexts.get();
        LRAContext context = findContext(current, lra);

        if (current == null || context == null) {
            return false;
        }

        current.stack.remove(context);

        // pop children
        // since child LRAs are contingent upon the parent, popping a parent should also pop the children

        // check every LRA associated with the calling thread and if it is a child of lra then pop it
        // the lra that is being popped is a parent of nextLRA:
        current.stack.removeIf(nextLRA -> isParentOf(lra, nextLRA.lraId()));

        if (current.stack.empty()) {
            clearContext(current);
        }

        return true;
    }

    /*
     * return true if child is nested under parent
     * ie if child has the parent anywhere in its parent tree
     */
    private static boolean isParentOf(UUID parent, UUID child) {
        if (parent == null || child == null) {
            return false; // child is top level
        }

        Current current = lraContexts.get();
        LRAContext context = findContext(current, child);

        if (context == null || context.parentLRA() == null) {
            return false; // reached the top of the hierarchy
        }

        if (parent.equals(context.parentLRA())) {
            return true;
        }

        // go through the whole parent tree recursively
        return isParentOf(parent, context.parentLRA());
    }

    /**
     * push the current context onto the stack of contexts for this thread
     *
     * @param lraId id of context to push (must not be null)
     * @param parentLRA parent context of the pushed LRA (null when top level)
     */
    public static void push(URI lraId, UUID parentLRA) {
        UUID id = lraId == null ? null : UUID.fromString(LRAConstants.getLRAUid(lraId));

        Current current = lraContexts.get();

        if (current == null) {
            lraContexts.set(new Current(id, parentLRA));
        } else if (findContext(current, id) == null) {
            current.stack.push(new LRAContext(id, parentLRA));
        }
    }

    public static List<Object> getContexts() {
        Current current = lraContexts.get();

        if (current == null) {
            return new ArrayList<>();
        }

        return current.stack.stream().map(context -> toURI(context.lraId())).collect(Collectors.toList());
    }

    /**
     * If there is an LRA context on the calling thread then add it to the provided headers
     *
     * @param responseContext the header map to add the KRA context to
     */
    public static void updateLRAContext(ContainerResponseContext responseContext) {
        UUID lraId = Current.peek();

        if (lraId != null) {
            responseContext.getHeaders().put(LRA_HTTP_CONTEXT_HEADER, getContexts());
        } else {
            responseContext.getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);
        }
    }

    public static void popAll() {
        lraContexts.remove();
    }

    public static <T> T getLast(List<T> objects) {
        return objects == null ? null : objects.stream().reduce((a, b) -> b).orElse(null);
    }
}
