/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.config;

import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Participant connector configuration keys and the small lookups the client-side builders need.
 */
public final class CallbackConfig {

    public static final String PROTOCOL_PROPERTY = "quarkus.lra.client.protocol";

    public static final String SERVICE_TOPIC_PROPERTY = "quarkus.lra.client-topic";

    public static final String HTTP = "http";

    public static final String KAFKA = "kafka";

    private CallbackConfig() {
    }

    public static String protocolId() {
        return ConfigProvider.getConfig()
                .getOptionalValue(PROTOCOL_PROPERTY, String.class)
                .filter(value -> !value.isBlank())
                .orElse(HTTP)
                .trim();
    }

    public static boolean isKafka() {
        return KAFKA.equalsIgnoreCase(protocolId());
    }

    public static String serviceTopic() {
        return ConfigProvider.getConfig().getValue(SERVICE_TOPIC_PROPERTY, String.class);
    }
}
