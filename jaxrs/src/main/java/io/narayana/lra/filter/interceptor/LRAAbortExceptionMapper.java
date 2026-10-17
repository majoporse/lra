/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.filter.interceptor;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * Maps the {@link LRAAbortException} thrown by {@link LRAInterceptor} onto the HTTP response that
 * the equivalent JAX-RS filter ({@link io.narayana.lra.filter.ServerLRAFilter}) would have produced
 * with {@code ContainerRequestContext.abortWith}.
 * <p>
 * A filter-less deployment has nothing else to translate the exception, so without this mapper an
 * intercepted method that had to be aborted (a violated propagation rule, an inactive incoming LRA,
 * and so on) would surface as a generic {@code 500} instead of the intended status code.
 */
@Provider
public class LRAAbortExceptionMapper implements ExceptionMapper<LRAAbortException> {

    @Override
    public Response toResponse(LRAAbortException exception) {
        return Response.status(exception.getStatusCode(), exception.getMessage()).build();
    }
}
