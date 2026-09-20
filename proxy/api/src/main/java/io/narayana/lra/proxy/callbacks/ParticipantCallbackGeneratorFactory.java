/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.proxy.callbacks;

import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Selects the {@link ParticipantCallbackGenerator} to use for a non-JAX-RS participant
 * based on configuration. The {@code quarkus.lra.listener.protocol} property selects the
 * transport: {@code http} (default) or {@code kafka}.
 */
public final class ParticipantCallbackGeneratorFactory {

    static final String LISTENER_PROTOCOL_PROPERTY = "quarkus.lra.listener.protocol";

    private ParticipantCallbackGeneratorFactory() {
    }

    public static ParticipantCallbackGenerator select() {
        String protocol = ConfigProvider.getConfig()
                .getOptionalValue(LISTENER_PROTOCOL_PROPERTY, String.class)
                .orElse("http");
        return "kafka".equalsIgnoreCase(protocol)
                ? new KafkaParticipantCallbackGenerator()
                : new HttpParticipantCallbackGenerator();
    }
}