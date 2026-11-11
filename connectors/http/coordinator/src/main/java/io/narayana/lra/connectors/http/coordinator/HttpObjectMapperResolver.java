/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.http.contracts.JoinLRAHttp;
import jakarta.ws.rs.ext.ContextResolver;
import jakarta.ws.rs.ext.Provider;

/**
 * Gives the JAX-RS provider a mapper that knows the connector callback-address modules.
 * <p>
 * The JAX-RS provider creates its own mapper and is not configured with the LRA connector modules.
 * The join body is the one HTTP payload that carries polymorphic callback addresses, so only that
 * type (and the address book it contains) is mapped by this resolver.
 */
@Provider
public class HttpObjectMapperResolver implements ContextResolver<ObjectMapper> {

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    @Override
    public ObjectMapper getContext(Class<?> type) {
        return JoinLRAHttp.Request.class.isAssignableFrom(type)
                || ParticipantCallbacks.class.isAssignableFrom(type)
                        ? mapper
                        : null;
    }
}
