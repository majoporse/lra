/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.jaxrs;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;

import io.narayana.lra.Current;
import io.narayana.lra.connectors.common.client.invocation.LraCompletion;
import io.narayana.lra.connectors.common.client.invocation.LraInvocation;
import io.narayana.lra.connectors.common.client.lifecycle.LifecycleDecision;
import io.narayana.lra.connectors.common.client.lifecycle.LraLifecycleProcessor;
import io.narayana.lra.logging.LRALogger;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import java.net.URI;
import java.util.List;

/**
 * The JAX-RS side of the LRA lifecycle. For a synchronous invocation the interceptor has already
 * completed the lifecycle; this filter only reports a lifecycle failure and publishes the effective
 * LRA. For an asynchronous invocation it completes the lifecycle once the final HTTP status is
 * known.
 * <p>
 * It contains no annotation handling and no lifecycle state machine. All decisions live in
 * {@link LraLifecycleProcessor}.
 */
@Provider
@ApplicationScoped
@Priority(Integer.MIN_VALUE) // response filters run in reverse priority order: run last
public class LraResponseFilter implements ContainerResponseFilter {

    static final String PROPAGATION_PROPERTY = LraResponseFilter.class.getName() + ".propagation";
    static final String CACHE_PROPERTY = LraResponseFilter.class.getName() + ".cache";

    @Inject
    LraLifecycleProcessor processor;

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
        if (requestContext.getProperty(PROPAGATION_PROPERTY) != null) {
            // the request filter pushed the incoming LRA for an unannotated resource
            requestContext.removeProperty(PROPAGATION_PROPERTY);

            Object cache = requestContext.getProperty(CACHE_PROPERTY);

            if (cache != null) {
                requestContext.removeProperty(CACHE_PROPERTY);
                Current.removeActiveLRACache(URI.create(cache.toString()));
            }

            Current.popAll();
            Current.clearAuthToken();
            return;
        }

        Object property = requestContext.getProperty(LraInvocation.PROPERTY);

        if (!(property instanceof LraInvocation invocation)) {
            // the interceptor did not manage this invocation
            return;
        }

        try {
            if (!invocation.isCompleted()) {
                LifecycleDecision decision = processor.complete(invocation,
                        LraCompletion.ofStatus(responseContext.getStatus(), null));

                if (decision.hasFailure()) {
                    responseContext.setEntity(decision.failureMessage());
                    LRALogger.logger.warn(decision.failureMessage());
                }

                invocation.markCompleted();
            } else if (invocation.getLifecycleFailure() != null) {
                responseContext.setEntity(invocation.getLifecycleFailure());
                LRALogger.logger.warn(invocation.getLifecycleFailure());
            }

            updateResponseHeader(invocation, responseContext);
        } finally {
            // the HTTP request boundary ends every context this request created
            Current.popAll();
            Current.removeActiveLRACache(invocation.getCurrentLRA());
            Current.clearAuthToken();
            requestContext.removeProperty(LraInvocation.PROPERTY);
        }
    }

    private static void updateResponseHeader(LraInvocation invocation, ContainerResponseContext responseContext) {
        // a nested/requires-new/not-supported invocation suspended the caller's LRA: the response
        // advertises the LRA the caller is still in, never the one this invocation ended
        URI lraId = invocation.isSuspendedLRA() && invocation.getIncomingLRA() != null
                ? invocation.getIncomingLRA()
                : invocation.getCurrentLRA();

        if (lraId != null) {
            responseContext.getHeaders().put(LRA_HTTP_CONTEXT_HEADER, List.of(lraId));
        } else {
            responseContext.getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);
        }
    }
}
