/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

import java.net.URI;
import java.util.ServiceLoader;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Factory for creating {@link LRAClient} instances.
 *
 * <p>
 * The implementation is selected by the {@code lra.transport.type} configuration property:
 * <ul>
 * <li>{@code http} (default) — creates a {@link NarayanaLRAClient}</li>
 * <li>{@code messaging} — loads a messaging implementation via {@link ServiceLoader}</li>
 * </ul>
 * </p>
 */
public class LRAClientFactory {

    private static final String TRANSPORT_TYPE_PROPERTY = "lra.transport.type";
    private static final String TRANSPORT_HTTP = "http";
    private static final String TRANSPORT_MESSAGING = "messaging";

    /**
     * Create an LRAClient using the default coordinator URL from configuration.
     *
     * @return the LRAClient implementation
     */
    public static LRAClient createClient() {
        return createClient(null);
    }

    /**
     * Create an LRAClient with an explicit coordinator URL.
     *
     * @param coordinatorUri the coordinator URI, or null to use configuration default
     * @return the LRAClient implementation
     */
    public static LRAClient createClient(URI coordinatorUri) {
        String type = getTransportType();

        switch (type) {
            case TRANSPORT_MESSAGING:
                return createMessagingClient();
            case TRANSPORT_HTTP:
            default:
                return coordinatorUri != null
                        ? new NarayanaLRAClient(coordinatorUri)
                        : new NarayanaLRAClient();
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

    private static LRAClient createMessagingClient() {
        try {
            ServiceLoader<LRAClient> loader = ServiceLoader.load(LRAClient.class);
            for (LRAClient client : loader) {
                if (!(client instanceof NarayanaLRAClient)) {
                    return client;
                }
            }
            throw new IllegalStateException("No messaging LRAClient implementation found via ServiceLoader. "
                    + "Ensure lra-messaging module is on the classpath.");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create messaging LRAClient", e);
        }
    }
}
