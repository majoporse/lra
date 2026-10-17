/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.filter.interceptor;

import io.narayana.lra.Current;
import java.net.URI;
import java.util.List;

/**
 * Transport-neutral access to the LRA metadata associated with the invocation currently being
 * processed by {@link LRAInterceptor}.
 * <p>
 * The names used are the canonical header names defined by the MicroProfile LRA specification
 * (for example {@code Long-Running-Action}). HTTP maps them onto request/response headers,
 * messaging systems onto record/message headers and gRPC onto metadata. Each transport supplies
 * its own implementation of this interface; when none is present the default
 * {@link HttpLRATransportContext}, which reads the JAX-RS request and falls back to the
 * {@link Current} thread-local, is used.
 * <p>
 * Only the inbound side is modelled for now. Facilities for writing the LRA context onto an
 * outgoing message will be added together with the transports that require them.
 */
public interface LRATransportContext {

    /**
     * @param name the canonical LRA header name
     * @return all the values associated with the given name, or an empty list when there are none
     */
    List<String> getHeaders(String name);

    /**
     * Convenience accessor returning the last value associated with the given name.
     *
     * @param name the canonical LRA header name
     * @return the last value or {@code null} when there is none
     */
    default String getHeader(String name) {
        return Current.getLast(getHeaders(name));
    }

    /**
     * @return the base URI used to build URIs for LRA participant callbacks, or {@code null} when
     *         not configured; a deployment setting, never derived from the current invocation
     */
    URI getBaseUri();
}
