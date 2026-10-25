/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.internal;

import io.narayana.lra.connectors.common.client.CallbackPaths;
import io.narayana.lra.connectors.common.client.LRAClient;
import io.narayana.lra.connectors.common.client.ParticipantCallbackAddressResolver;
import io.narayana.lra.connectors.common.client.ParticipantConnector;
import io.narayana.lra.connectors.common.config.CallbackConfig;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.CDI;
import java.lang.annotation.Annotation;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Selects the participant's connector by {@code quarkus.lra.client.protocol}.
 * <p>
 * Selection is a runtime decision. It works the same under Weld and Arc, and it does not
 * instantiate the connectors that are not selected (a Kafka client may need configuration that is
 * absent when the participant protocol is HTTP).
 * <p>
 * The class has no injected state so the CDI extension can register it with a plain instance; it
 * resolves the container lazily when a selection is requested.
 */
public class ParticipantProtocolRegistry {

    public String protocolId() {
        String configured = ConfigProvider.getConfig()
                .getOptionalValue(CallbackConfig.PROTOCOL_PROPERTY, String.class)
                .orElse(CallbackPaths.HTTP);

        if (configured == null || configured.isBlank()) {
            throw new IllegalStateException("Missing value for '" + CallbackConfig.PROTOCOL_PROPERTY
                    + "'. Registered connector ids: " + connectorIds());
        }

        return configured.trim();
    }

    public LRAClient client() {
        return select(CDI.current().select(LRAClient.class), "client");
    }

    public ParticipantCallbackAddressResolver resolver() {
        return select(CDI.current().select(ParticipantCallbackAddressResolver.class), "resolver");
    }

    public Set<String> connectorIds() {
        return connectorIds(CDI.current().select(LRAClient.class));
    }

    private <T> T select(Instance<T> instance, String role) {
        String id = protocolId();
        Instance<T> selected = instance.select(ParticipantConnector.Literal.of(id));

        if (selected.isUnsatisfied()) {
            throw new IllegalStateException("No LRA " + role + " registered for protocol '" + id
                    + "'. Registered connector ids: " + connectorIds(instance));
        }

        if (selected.isAmbiguous()) {
            throw new IllegalStateException("Multiple LRA " + role + " implementations registered for protocol '"
                    + id + "'");
        }

        return selected.get();
    }

    private static Set<String> connectorIds(Instance<?> instance) {
        return instance.handlesStream()
                .map(handle -> connectorId(handle.getBean().getQualifiers()))
                .filter(id -> id != null)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private static String connectorId(Set<Annotation> qualifiers) {
        for (Annotation qualifier : qualifiers) {
            if (qualifier instanceof ParticipantConnector connector) {
                return connector.value();
            }
        }

        return null;
    }
}
