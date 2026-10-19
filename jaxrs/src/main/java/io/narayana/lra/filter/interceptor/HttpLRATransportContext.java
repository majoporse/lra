/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.filter.interceptor;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;

import io.narayana.lra.Current;
import io.narayana.lra.LRAConstants;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import java.net.URI;
import java.util.List;

/**
 * Default, HTTP aware {@link LRATransportContext}.
 * <p>
 * It reads the LRA metadata straight from the JAX-RS request that is in progress, so an intercepted
 * bean can discover an incoming LRA without any help from a JAX-RS filter. The request context is
 * obtained by CDI injection of {@link HttpHeaders}; that type is provided as a request scoped bean
 * by the Jakarta REST implementations that support CDI (for example RESTEasy's {@code resteasy-cdi}
 * {@code ContextProducers}, which is what runs on WildFly and Quarkus RESTEasy). When no request is
 * in progress the injection is unsatisfied or its scope is inactive, and the class falls back to the
 * {@link Current} thread local.
 * <p>
 * The effective LRA (as tracked by {@link Current}) takes precedence over the raw request header: a
 * method may have started a new LRA or suspended the incoming one, and the intercepted method must
 * join the effective context, not whatever the caller sent. Transports that carry LRA metadata on a
 * different kind of message provide their own implementation of {@link LRATransportContext}.
 */
@ApplicationScoped
public class HttpLRATransportContext implements LRATransportContext {

    @Inject
    Instance<HttpHeaders> httpHeaders;

    @Override
    public List<String> getHeaders(String name) {
        if (LRA_HTTP_CONTEXT_HEADER.equals(name)) {
            URI current = Current.peek();

            if (current != null) {
                return List.of(current.toASCIIString());
            }
        }

        List<String> values = requestHeaders(name);

        return values == null ? List.of() : values;
    }

    /**
     * @return the configured {@code narayana.lra.base-uri}, or {@code null} when not set; never
     *         derived from the in-flight request
     */
    @Override
    public URI getBaseUri() {
        return LRAConstants.getBaseUri().map(URI::create).orElse(null);
    }

    private List<String> requestHeaders(String name) {
        try {
            HttpHeaders headers = httpHeaders();

            return headers == null ? null : headers.getRequestHeader(name);
        } catch (IllegalStateException e) {
            // the request is no longer available (for example after the response has been written)
            return null;
        }
    }

    private HttpHeaders httpHeaders() {
        try {
            return httpHeaders.isResolvable() ? httpHeaders.get() : null;
        } catch (ContextNotActiveException e) {
            // no request scoped context is active (for example a plain CDI invocation)
            return null;
        }
    }
}