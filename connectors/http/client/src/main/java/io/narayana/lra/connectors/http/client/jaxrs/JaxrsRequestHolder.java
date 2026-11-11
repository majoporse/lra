/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.jaxrs;

import jakarta.enterprise.context.RequestScoped;
import jakarta.ws.rs.container.ContainerRequestContext;

/**
 * Request-scoped holder for the JAX-RS request context of the current invocation.
 * <p>
 * An interceptor cannot inject {@link ContainerRequestContext} portably, but it runs on the
 * request thread, so a thin request filter can capture the context here and the transport can
 * store its {@link io.narayana.lra.connectors.common.client.invocation.LraInvocationState} on it. The
 * request context (and its properties) survives into the response phase, including asynchronous
 * responses resumed on another thread.
 */
@RequestScoped
public class JaxrsRequestHolder {

    private ContainerRequestContext requestContext;

    public ContainerRequestContext getRequestContext() {
        return requestContext;
    }

    public void setRequestContext(ContainerRequestContext requestContext) {
        this.requestContext = requestContext;
    }
}
