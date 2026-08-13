/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;

import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.MultivaluedMap;
import java.net.URI;
import java.util.List;

/**
 * HTTP implementation of {@link LRAContextPropagator}.
 * Reads/writes the {@code Long-Running-Action} HTTP header.
 */
public class LRAContextPropagatorHTTP implements LRAContextPropagator {

    @Override
    public URI readIncomingContext(Object message) {
        if (message instanceof ContainerRequestContext) {
            ContainerRequestContext requestContext = (ContainerRequestContext) message;
            List<String> headers = requestContext.getHeaders().get(LRA_HTTP_CONTEXT_HEADER);
            if (headers != null && !headers.isEmpty()) {
                String header = headers.get(headers.size() - 1);
                if (header != null && !header.isEmpty()) {
                    try {
                        return URI.create(header);
                    } catch (IllegalArgumentException e) {
                        return null;
                    }
                }
            }
        }
        return null;
    }

    @Override
    public void writeOutgoingContext(Object message, URI lraId) {
        if (message instanceof ClientRequestContext) {
            ClientRequestContext clientContext = (ClientRequestContext) message;
            if (lraId != null) {
                clientContext.getHeaders().putSingle(LRA_HTTP_CONTEXT_HEADER, lraId.toString());
            }
        }
    }

    @Override
    public void clearContext(Object message) {
        if (message instanceof ClientRequestContext) {
            ((ClientRequestContext) message).getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);
        }
    }

    /**
     * Update the LRA context in HTTP response headers.
     * Moved from {@code Current.updateLRAContext(ContainerResponseContext)}.
     */
    public static void updateResponseContext(ContainerResponseContext responseContext, URI lraId) {
        if (lraId != null) {
            responseContext.getHeaders().putSingle(LRA_HTTP_CONTEXT_HEADER, lraId.toString());
        } else {
            responseContext.getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);
        }
    }

    /**
     * Update the LRA context in a header map.
     * Moved from {@code Current.updateLRAContext(URI, MultivaluedMap)}.
     */
    public static void updateHeaders(MultivaluedMap<String, String> headers, URI lraId) {
        headers.putSingle(LRA_HTTP_CONTEXT_HEADER, lraId.toString());
    }

    /**
     * Clear LRA context from a header map.
     * Moved from {@code Current.clearContext(MultivaluedMap)}.
     */
    public static void clearHeaders(MultivaluedMap<String, String> headers) {
        headers.remove(LRA_HTTP_CONTEXT_HEADER);
    }
}
