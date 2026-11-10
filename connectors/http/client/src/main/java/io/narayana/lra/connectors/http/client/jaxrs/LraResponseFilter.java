/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.jaxrs;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;

import io.narayana.lra.Current;
import io.narayana.lra.connectors.common.client.invocation.LraInvocation;
import io.narayana.lra.connectors.common.client.lifecycle.LifecycleDecision;
import io.narayana.lra.connectors.common.client.lifecycle.LraLifecycleProcessor;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import java.net.URI;
import java.util.List;

/**
 * The JAX-RS side of the LRA lifecycle: it completes the invocation the interceptor started, once
 * the final HTTP status is known.
 * <p>
 * It contains no annotation handling and no lifecycle state machine. It reads the state the
 * interceptor stored on the request, translates the status into a cancel decision using the rules
 * the processor already resolved, publishes the effective LRA on the response and restores the
 * thread-local state. All decisions live in {@link LraLifecycleProcessor}.
 */
@Provider
@ApplicationScoped
public class LraResponseFilter implements ContainerResponseFilter {

    static final String PROPAGATION_PROPERTY = LraResponseFilter.class.getName() + ".propagation";

    @Inject
    LraLifecycleProcessor processor;

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
        if (requestContext.getProperty(PROPAGATION_PROPERTY) != null) {
            // the request filter pushed the incoming LRA for an unannotated resource
            requestContext.removeProperty(PROPAGATION_PROPERTY);
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
            LifecycleDecision decision = processor.processResponse(invocation,
                    processor.isCancel(invocation, responseContext.getStatus()));

            if (decision.hasFailure()) {
                responseContext.setEntity(decision.failureMessage());
                LRALogger.logger.warn(decision.failureMessage());
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
