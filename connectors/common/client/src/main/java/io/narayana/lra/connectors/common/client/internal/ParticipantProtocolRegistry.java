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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Inject;
import java.lang.annotation.Annotation;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Selects the participant's connector by {@code quarkus.lra.client.protocol}.
 * <p>
 * Selection is a runtime decision. The injected {@link Instance} keeps every connector bean visible
 * to the CDI container (Quarkus Arc would otherwise remove beans used only through programmatic
 * lookup), but a connector is not instantiated until it is selected.
 * <p>
 * The class is a normal CDI bean so both Weld and Arc discover it; the portable CDI extension does
 * not register it.
 */
@ApplicationScoped
public class ParticipantProtocolRegistry {

    @Inject
    @Any
    Instance<LRAClient> clients;

    @Inject
    @Any
    Instance<ParticipantCallbackAddressResolver> resolvers;

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
        return select(clients(), "client");
    }

    public ParticipantCallbackAddressResolver resolver() {
        return select(resolvers(), "resolver");
    }

    public Set<String> connectorIds() {
        return connectorIds(clients());
    }

    // the fallback keeps manually constructed registries (unit tests, non-CDI callers) working
    private Instance<LRAClient> clients() {
        return clients != null ? clients : CDI.current().select(LRAClient.class, Any.Literal.INSTANCE);
    }

    private Instance<ParticipantCallbackAddressResolver> resolvers() {
        return resolvers != null ? resolvers
                : CDI.current().select(ParticipantCallbackAddressResolver.class, Any.Literal.INSTANCE);
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
