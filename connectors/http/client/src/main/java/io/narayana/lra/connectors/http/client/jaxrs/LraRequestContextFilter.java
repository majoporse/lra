/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.jaxrs;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;

import io.narayana.lra.AnnotationResolver;
import io.narayana.lra.Current;
import io.narayana.lra.connectors.common.client.interceptor.LRAAnnotationResolver;
import io.narayana.lra.connectors.common.client.invocation.ServerLRAContext;
import io.narayana.lra.connectors.common.client.lifecycle.LraLifecycleProcessor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import java.lang.reflect.Method;
import java.net.URI;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;

/**
 * The JAX-RS side of the request: it captures the request context for the interceptor and
 * implements the propagation rule for resource methods that carry no LRA annotation.
 * <p>
 * For an annotated method the interceptor owns the lifecycle. For an unannotated resource the
 * MicroProfile propagation rule requires the incoming LRA to be hidden from the method (the
 * {@code Long-Running-Action} header is removed) while remaining available to outgoing calls (it
 * is pushed onto {@link Current}); the response filter pops it.
 * <p>
 * A {@code @Leave} method on a bean with no {@code @LRA} declaration is the exception the filter
 * handles itself: the interceptor is bound through {@code @LRA}, so it would never run for such a
 * method. The leave is delegated to the same processor the interceptor uses.
 */
@Provider
@ApplicationScoped
public class LraRequestContextFilter implements ContainerRequestFilter {

    @Inject
    LraInvocationState invocationState;

    @Inject
    LRAAnnotationResolver resolver;

    @Inject
    LraLifecycleProcessor processor;

    @Context
    ResourceInfo resourceInfo;

    @Override
    public void filter(ContainerRequestContext requestContext) {
        invocationState.setRequestContext(requestContext);

        Method method = resourceInfo == null ? null : resourceInfo.getResourceMethod();

        if (method != null && isLeaveOnly(method)) {
            leave(requestContext, method);
            return;
        }

        if (method == null || isManaged(method)) {
            return;
        }

        String incoming = requestContext.getHeaderString(LRA_HTTP_CONTEXT_HEADER);

        if (incoming == null) {
            return;
        }

        requestContext.getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);
        Current.push(URI.create(incoming), null);
        requestContext.setProperty(LraResponseFilter.PROPAGATION_PROPERTY, incoming);
    }

    private boolean isManaged(Method method) {
        // participant callbacks run with the incoming LRA context and must still see the header
        return resolver.isParticipantCallback(method)
                || AnnotationResolver.isAnnotationPresent(LRA.class, method)
                || method.getDeclaringClass().isAnnotationPresent(LRA.class)
                || resolver.isLeave(method)
                || resolver.propagateToken(method);
    }

    /**
     * The interceptor is bound through {@code @LRA}, so a {@code @Leave} method on a bean without
     * any {@code @LRA} declaration would never be intercepted. Such methods are handled here,
     * before JAX-RS invokes the business method.
     */
    private boolean isLeaveOnly(Method method) {
        return resolver.isLeave(method)
                && !AnnotationResolver.isAnnotationPresent(LRA.class, method)
                && !method.getDeclaringClass().isAnnotationPresent(LRA.class);
    }

    private void leave(ContainerRequestContext requestContext, Method method) {
        String incoming = requestContext.getHeaderString(LRA_HTTP_CONTEXT_HEADER);

        if (incoming == null) {
            return;
        }

        ServerLRAContext context = new ServerLRAContext(method, resourceInfo.getResourceClass());
        context.setIncomingLRA(URI.create(incoming));

        processor.processRequest(context);

        if (context.isAborted()) {
            requestContext.abortWith(Response.status(context.getAbortStatusCode())
                    .entity(context.getAbortMessage()).build());
            return;
        }

        // the processor pushed the incoming context; the response filter only has to clean it up
        requestContext.setProperty(LraResponseFilter.PROPAGATION_PROPERTY, incoming);
    }
}
