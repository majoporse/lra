/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.jaxrs;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;

import io.narayana.lra.Current;
import io.narayana.lra.connectors.common.client.interceptor.LRAAnnotationResolver;
import io.narayana.lra.connectors.common.client.invocation.LraMethodMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.ext.Provider;
import java.lang.reflect.Method;
import java.net.URI;

/**
 * The JAX-RS side of the request: it captures the request context for the interceptor and
 * implements the propagation rule for resource methods that carry no LRA annotation.
 * <p>
 * For an annotated method the interceptor owns the lifecycle. For an unannotated resource the
 * MicroProfile propagation rule requires the incoming LRA to be hidden from the method (the
 * {@code Long-Running-Action} header is removed) while remaining available to outgoing calls (it
 * is pushed onto {@link Current}); the response filter pops it.
 */
@Provider
@ApplicationScoped
public class LraRequestContextFilter implements ContainerRequestFilter {

    @Inject
    LraInvocationState invocationState;

    @Inject
    LRAAnnotationResolver resolver;

    @Context
    ResourceInfo resourceInfo;

    @Override
    public void filter(ContainerRequestContext requestContext) {
        invocationState.setRequestContext(requestContext);

        Method method = resourceInfo == null ? null : resourceInfo.getResourceMethod();

        if (method == null) {
            return;
        }

        LraMethodMetadata metadata = resolver.resolve(method);

        if (metadata.isManaged() || metadata.isParticipantCallback()) {
            return;
        }

        String incoming = requestContext.getHeaderString(LRA_HTTP_CONTEXT_HEADER);

        if (incoming == null) {
            return;
        }

        requestContext.getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);
        URI lraId = URI.create(incoming);
        Current.push(lraId, null);
        Current.addActiveLRACache(lraId);
        requestContext.setProperty(LraResponseFilter.PROPAGATION_PROPERTY, incoming);
        requestContext.setProperty(LraResponseFilter.CACHE_PROPERTY, incoming);
    }
}
