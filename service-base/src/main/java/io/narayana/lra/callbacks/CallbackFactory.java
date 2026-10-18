/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.callbacks;

import io.narayana.lra.LRAConstants;
import java.net.URI;
import java.util.Map;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Creates the {@link LRACallback}s a participant enlists with, choosing the callback protocol
 * from {@code quarkus.lra.client.protocol} (default {@code http}). For {@code http} callbacks
 * carry the coordinator-reachable address of the participant endpoint; for {@code kafka} they
 * carry the participant's callback topic (see the "Callback protocol" glossary term).
 * <p>
 * A protocol is never mixed: the same property that selects the client's coordinator transport
 * also selects how its participant callbacks are delivered.
 */
public class CallbackFactory {

    public static final String PROTOCOL_PROPERTY = "quarkus.lra.client.protocol";
    public static final String CALLBACK_TOPIC_PROPERTY = "quarkus.lra.client-callback-topic";

    private final Protocol protocol;
    private final String kafkaTopic;

    public CallbackFactory() {
        String configured = ConfigProvider.getConfig()
                .getOptionalValue(PROTOCOL_PROPERTY, String.class)
                .filter(value -> !value.isBlank())
                .orElse(Protocol.HTTP.value());

        try {
            this.protocol = Protocol.from(configured);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Unsupported value '" + configured + "' for '" + PROTOCOL_PROPERTY
                    + "'. Supported values are '" + Protocol.HTTP.value() + "' and '" + Protocol.KAFKA.value() + "'.",
                    e);
        }

        this.kafkaTopic = protocol == Protocol.KAFKA
                ? ConfigProvider.getConfig().getValue(CALLBACK_TOPIC_PROPERTY, String.class)
                : null;
    }

    /**
     * The callback for an endpoint addressed by an explicit URI. The URI is used for the HTTP
     * protocol; for Kafka the callback carries the participant's configured callback topic.
     */
    public LRACallback of(CallbackType type, URI explicitUri) {
        return protocol == Protocol.KAFKA
                ? KafkaCallback.of(type, kafkaTopic)
                : HttpCallback.of(type, explicitUri);
    }

    /**
     * The callback for an endpoint at {@code pathOrNull} under the configured base URI (see
     * {@code narayana.lra.base-uri}). When {@code pathOrNull} is null the callback kind's path
     * segment is used. This is the seam for standardising participant callbacks on
     * {@code <base>/<callback-kind>} once per-method {@code @Path} values are dropped.
     */
    public LRACallback of(CallbackType type, String pathOrNull) {
        return of(type, baseUri(), pathOrNull);
    }

    /**
     * The callback for an endpoint at {@code pathOrNull} under {@code baseUri}.
     */
    public LRACallback of(CallbackType type, URI baseUri, String pathOrNull) {
        if (protocol == Protocol.KAFKA) {
            return KafkaCallback.of(type, kafkaTopic);
        }
        String path = pathOrNull == null || pathOrNull.isBlank() ? type.rel() : pathOrNull;
        return HttpCallback.of(type, join(baseUri, path));
    }

    /**
     * Builds the non-null callbacks in {@code callbacks} from the given per-kind URIs.
     */
    public void fill(ParticipantCallbacks callbacks, Map<CallbackType, URI> explicitUris) {
        for (Map.Entry<CallbackType, URI> entry : explicitUris.entrySet()) {
            callbacks.callback(entry.getKey(), of(entry.getKey(), entry.getValue()));
        }
    }

    private static URI join(URI baseUri, String path) {
        String base = baseUri == null ? "" : baseUri.toASCIIString();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return URI.create(base + (path.startsWith("/") ? path : "/" + path));
    }

    private static URI baseUri() {
        return LRAConstants.getBaseUri()
                .map(URI::create)
                .orElse(URI.create("http://localhost:8080/" + LRAConstants.COORDINATOR_PATH_NAME));
    }

}
