/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.config;

import io.narayana.lra.connectors.common.config.CallbackConfig;
import io.smallrye.config.ConfigSourceInterceptor;
import io.smallrye.config.ConfigSourceInterceptorContext;
import io.smallrye.config.ConfigValue;

/**
 * Derives the boolean {@code quarkus.lra.client.protocol.kafka} from the configured callback
 * protocol. SmallRye messaging channels need a boolean {@code enabled} flag but the user-facing
 * knob is the protocol name, and SmallRye's {@code BooleanConverter} would read the string
 * {@code "kafka"} as {@code false}; this interceptor keeps the two in step (see the
 * "One callback protocol per client" ADR).
 * <p>
 * Registered via {@code META-INF/services/io.smallrye.config.ConfigSourceInterceptor}.
 */
public class ProtocolConfigInterceptor implements ConfigSourceInterceptor {

    public static final String KAFKA_PROPERTY = "quarkus.lra.client.protocol.kafka";

    @Override
    public ConfigValue getValue(ConfigSourceInterceptorContext context, String name) {
        if (!KAFKA_PROPERTY.equals(name)) {
            return context.proceed(name);
        }

        ConfigValue protocol = context.proceed(CallbackConfig.PROTOCOL_PROPERTY);
        String value = protocol != null ? protocol.getValue() : null;

        if (value == null || value.isBlank()) {
            return ConfigValue.builder().withName(name).withValue("false").build();
        }

        String id = value.trim();

        if (!CallbackConfig.HTTP.equalsIgnoreCase(id) && !CallbackConfig.KAFKA.equalsIgnoreCase(id)) {
            throw new IllegalArgumentException("Unsupported value '" + id + "' for '"
                    + CallbackConfig.PROTOCOL_PROPERTY + "'. Supported values are '"
                    + CallbackConfig.HTTP + "' and '" + CallbackConfig.KAFKA + "'.");
        }

        boolean kafka = CallbackConfig.KAFKA.equalsIgnoreCase(id);

        return ConfigValue.builder().withName(name).withValue(String.valueOf(kafka)).build();
    }
}
