/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.filter.interceptor;

/**
 * Thrown by {@link LRAInterceptor} when the LRA processing prevented the intercepted method from
 * being invoked (the transport-neutral equivalent of the JAX-RS
 * {@code ContainerRequestContext.abortWith} call performed by
 * {@link io.narayana.lra.filter.ServerLRAFilter}).
 * <p>
 * The status code carries the same value that would have been used for the HTTP response so that a
 * transport adapter can map it to the appropriate error.
 */
public class LRAAbortException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int statusCode;

    public LRAAbortException(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
