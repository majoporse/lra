/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.participant;

import io.narayana.lra.AnnotationResolver;
import io.narayana.lra.LRAConstants;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.client.participant.Participant;
import io.narayana.lra.connectors.common.client.participant.ParticipantCallbackAddressResolver;
import io.narayana.lra.connectors.common.client.participant.protocol.CallbackPaths;
import io.narayana.lra.connectors.common.client.participant.protocol.ParticipantConnector;
import io.narayana.lra.connectors.http.address.HttpContractAddress;
import io.narayana.lra.connectors.http.address.LegacyHttpAddress;
import io.narayana.lra.connectors.http.support.HttpContractPaths;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

/**
 * Writes the HTTP callback addresses of a participant.
 * <p>
 * A MicroProfile callback method that declares {@code @Path} and an HTTP verb is the participant's
 * own endpoint and is registered as a {@link LegacyHttpAddress}. Every other callback is served by
 * the common callback proxy and is registered as an {@link HttpContractAddress}
 * ({@code {base}/lra-callback-proxy/{participantId}/{op}}).
 * <p>
 * The base the addresses are built from is {@code narayana.lra.base-uri} when configured, otherwise
 * the base URI of the request being processed. Having neither is an error: enlisting a participant
 * the coordinator cannot call back would be worse than failing.
 */
@ApplicationScoped
@ParticipantConnector(CallbackPaths.HTTP)
public class HttpAddressResolver implements ParticipantCallbackAddressResolver {

    @Inject
    Instance<UriInfo> uriInfo;

    @Override
    public ParticipantCallbacks resolve(Participant participant) {
        ParticipantCallbacks callbacks = new ParticipantCallbacks();
        URI base = baseAddress();

        Map<CallbackType, String> ownEndpoints = ownEndpointPaths(participant.participantClass());
        URI endpointPrefix = endpointPrefix(base, participant.participantClass());

        for (CallbackType type : CallbackType.values()) {
            if (!participant.hasCallback(type)) {
                continue;
            }

            String path = ownEndpoints.get(type);

            if (path == null) {
                callbacks.callback(type, new HttpContractAddress(type, participant.participantId(),
                        UriBuilder.fromUri(base)
                                .path(HttpContractPaths.RESOURCE_PATH)
                                .path(participant.participantId())
                                .path(type.rel())
                                .build()));
            } else {
                callbacks.callback(type, LegacyHttpAddress.of(type,
                        UriBuilder.fromUri(endpointPrefix).path(path).build(), participant.participantId()));
            }
        }

        return callbacks;
    }

    private URI baseAddress() {
        Optional<String> configured = LRAConstants.getBaseUri();

        if (configured.isPresent()) {
            return URI.create(configured.get());
        }

        try {
            if (uriInfo != null && uriInfo.isResolvable()) {
                return uriInfo.get().getBaseUri();
            }
        } catch (ContextNotActiveException | IllegalStateException e) {
            // no request in progress; the configuration is the only remaining source
        }

        throw new IllegalStateException("No HTTP callback base address is available: set '"
                + LRAConstants.NARAYANA_LRA_BASE_URI_PROPERTY_NAME
                + "' or invoke the participant through a JAX-RS request");
    }

    // the participant's own endpoints live under its JAX-RS resource class path
    private static URI endpointPrefix(URI base, Class<?> javaClass) {
        Path classPath = javaClass.getAnnotation(Path.class);

        return classPath == null ? base : UriBuilder.fromUri(base).path(classPath.value()).build();
    }

    private static Map<CallbackType, String> ownEndpointPaths(Class<?> javaClass) {
        Map<CallbackType, String> paths = new EnumMap<>(CallbackType.class);

        for (Method method : javaClass.getMethods()) {
            CallbackType type = MicroProfileCallbackScan.callbackTypeOf(method);

            if (type == null || MicroProfileCallbackScan.isContractShape(method)
                    || !MicroProfileCallbackScan.hasHttpVerb(method)) {
                continue;
            }

            Path path = AnnotationResolver.resolveAnnotation(Path.class, method);

            if (path != null) {
                paths.put(type, MicroProfileCallbackScan.normalise(path.value()));
            }
        }

        return paths;
    }
}
