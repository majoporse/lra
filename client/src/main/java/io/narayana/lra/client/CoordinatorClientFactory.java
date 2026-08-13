/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

import java.net.URI;
import java.util.ServiceLoader;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Factory for creating transport-specific implementations of
 * {@link CoordinatorClient}, {@link LRAContextPropagator}, and {@link CallbackRegistrar}.
 *
 * <p>
 * The transport type is read from the {@code lra.transport.type} configuration property.
 * Valid values: {@code http} (default), {@code messaging}.
 * </p>
 */
public class CoordinatorClientFactory {

    public static final String TRANSPORT_TYPE_PROPERTY = "lra.transport.type";
    public static final String TRANSPORT_HTTP = "http";
    public static final String TRANSPORT_MESSAGING = "messaging";

    /**
     * Create a CoordinatorClient based on the configured transport type.
     *
     * @param coordinatorUri the coordinator URI (used by HTTP transport)
     * @return the CoordinatorClient implementation
     */
    public static CoordinatorClient createClient(URI coordinatorUri) {
        String type = getTransportType();

        switch (type) {
            case TRANSPORT_MESSAGING:
                return createMessagingClient();
            case TRANSPORT_HTTP:
            default:
                return new CoordinatorClientHTTP(coordinatorUri);
        }
    }

    /**
     * Create a LRAContextPropagator based on the configured transport type.
     *
     * @return the LRAContextPropagator implementation
     */
    public static LRAContextPropagator createContextPropagator() {
        String type = getTransportType();

        switch (type) {
            case TRANSPORT_MESSAGING:
                return createMessagingContextPropagator();
            case TRANSPORT_HTTP:
            default:
                return new LRAContextPropagatorHTTP();
        }
    }

    /**
     * Create a CallbackRegistrar based on the configured transport type.
     *
     * @return the CallbackRegistrar implementation
     */
    public static CallbackRegistrar createCallbackRegistrar() {
        String type = getTransportType();

        switch (type) {
            case TRANSPORT_MESSAGING:
                return createMessagingCallbackRegistrar();
            case TRANSPORT_HTTP:
            default:
                return new CallbackRegistrarHTTP();
        }
    }

    /**
     * Get the configured transport type.
     *
     * @return "http" or "messaging"
     */
    public static String getTransportType() {
        return ConfigProvider.getConfig()
                .getOptionalValue(TRANSPORT_TYPE_PROPERTY, String.class)
                .orElse(TRANSPORT_HTTP);
    }

    /**
     * Check if the messaging transport is configured.
     */
    public static boolean isMessagingTransport() {
        return TRANSPORT_MESSAGING.equals(getTransportType());
    }

    /**
     * Check if the HTTP transport is configured.
     */
    public static boolean isHttpTransport() {
        return TRANSPORT_HTTP.equals(getTransportType()) || !isMessagingTransport();
    }

    private static CoordinatorClient createMessagingClient() {
        try {
            ServiceLoader<CoordinatorClient> loader = ServiceLoader.load(CoordinatorClient.class);
            for (CoordinatorClient client : loader) {
                if (!(client instanceof CoordinatorClientHTTP)) {
                    return client;
                }
            }
            throw new IllegalStateException("No messaging CoordinatorClient implementation found via ServiceLoader. "
                    + "Ensure lra-messaging module is on the classpath.");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create messaging CoordinatorClient", e);
        }
    }

    private static LRAContextPropagator createMessagingContextPropagator() {
        try {
            ServiceLoader<LRAContextPropagator> loader = ServiceLoader.load(LRAContextPropagator.class);
            for (LRAContextPropagator propagator : loader) {
                if (!(propagator instanceof LRAContextPropagatorHTTP)) {
                    return propagator;
                }
            }
            throw new IllegalStateException("No messaging LRAContextPropagator implementation found via ServiceLoader.");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create messaging LRAContextPropagator", e);
        }
    }

    private static CallbackRegistrar createMessagingCallbackRegistrar() {
        try {
            ServiceLoader<CallbackRegistrar> loader = ServiceLoader.load(CallbackRegistrar.class);
            for (CallbackRegistrar registrar : loader) {
                if (!(registrar instanceof CallbackRegistrarHTTP)) {
                    return registrar;
                }
            }
            throw new IllegalStateException("No messaging CallbackRegistrar implementation found via ServiceLoader.");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create messaging CallbackRegistrar", e);
        }
    }
}
