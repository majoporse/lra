/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.callbacks;

import io.narayana.lra.LRAConstants;
import java.net.URI;
import java.util.Map;
import org.eclipse.microprofile.config.ConfigProvider;

public class CallbackFactory {

    public static final String PROTOCOL_PROPERTY = "quarkus.lra.client.protocol";

    public static final String SERVICE_TOPIC_PROPERTY = "quarkus.lra.client-topic";

    private final Protocol protocol;
    private final String kafkaTopic;
    private final String participantId;
    private final boolean contractCallbacks;

    public CallbackFactory() {
        this(null, false);
    }

    public CallbackFactory(String participantId) {
        this(participantId, false);
    }

    public CallbackFactory(String participantId, boolean contractCallbacks) {
        this.participantId = participantId;
        this.contractCallbacks = contractCallbacks;

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
                ? ConfigProvider.getConfig().getValue(SERVICE_TOPIC_PROPERTY, String.class)
                : null;
    }

    public static CallbackFactory forContractParticipant(String participantId) {
        return new CallbackFactory(participantId, true);
    }

    public boolean isKafka() {
        return protocol == Protocol.KAFKA;
    }

    public boolean isContractCallbacks() {
        return contractCallbacks;
    }

    public LRACallback of(CallbackType type, URI explicitUri) {
        if (protocol == Protocol.KAFKA) {
            return KafkaCallback.of(type, kafkaTopic, participantId);
        }

        return contractCallbacks
                ? HttpCallback.of(type, explicitUri, participantId)
                : LegacyHttpCallback.of(type, explicitUri);
    }

    public LRACallback of(CallbackType type, String pathOrNull) {
        return of(type, baseUri(), pathOrNull);
    }

    public LRACallback of(CallbackType type, URI baseUri, String pathOrNull) {
        if (protocol == Protocol.KAFKA) {
            return KafkaCallback.of(type, kafkaTopic, participantId);
        }
        String path = pathOrNull == null || pathOrNull.isBlank() ? type.rel() : pathOrNull;
        URI endpoint = join(baseUri, path);
        return contractCallbacks
                ? HttpCallback.of(type, endpoint, participantId)
                : LegacyHttpCallback.of(type, endpoint);
    }

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
