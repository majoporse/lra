/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.config;

import io.narayana.lra.callbacks.CallbackFactory;
import io.narayana.lra.callbacks.Protocol;
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

        ConfigValue protocol = context.proceed(CallbackFactory.PROTOCOL_PROPERTY);
        String value = protocol != null ? protocol.getValue() : null;
        boolean kafka = value != null && !value.isBlank() && Protocol.from(value) == Protocol.KAFKA;

        return ConfigValue.builder().withName(name).withValue(String.valueOf(kafka)).build();
    }
}
