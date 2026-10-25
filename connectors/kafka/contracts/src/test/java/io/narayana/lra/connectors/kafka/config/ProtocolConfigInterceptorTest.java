/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.config;

import io.narayana.lra.connectors.common.config.CallbackConfig;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProtocolConfigInterceptorTest {

    @Test
    public void derivesTrueWhenKafka() {
        System.setProperty(CallbackConfig.PROTOCOL_PROPERTY, CallbackConfig.KAFKA);
        try {
            Assertions.assertTrue(
                    ConfigProvider.getConfig().getValue(ProtocolConfigInterceptor.KAFKA_PROPERTY, Boolean.class));
        } finally {
            System.clearProperty(CallbackConfig.PROTOCOL_PROPERTY);
        }
    }

    @Test
    public void derivesFalseForHttp() {
        System.setProperty(CallbackConfig.PROTOCOL_PROPERTY, CallbackConfig.HTTP);
        try {
            Assertions.assertFalse(
                    ConfigProvider.getConfig().getValue(ProtocolConfigInterceptor.KAFKA_PROPERTY, Boolean.class));
        } finally {
            System.clearProperty(CallbackConfig.PROTOCOL_PROPERTY);
        }
    }

    @Test
    public void derivesFalseWhenUnset() {
        System.clearProperty(CallbackConfig.PROTOCOL_PROPERTY);
        Assertions.assertFalse(
                ConfigProvider.getConfig().getValue(ProtocolConfigInterceptor.KAFKA_PROPERTY, Boolean.class));
    }

    @Test
    public void rejectsUnknownProtocol() {
        System.setProperty(CallbackConfig.PROTOCOL_PROPERTY, "carrier-pigeon");
        try {
            Assertions.assertThrows(IllegalArgumentException.class, () -> ConfigProvider.getConfig()
                    .getValue(ProtocolConfigInterceptor.KAFKA_PROPERTY, Boolean.class));
        } finally {
            System.clearProperty(CallbackConfig.PROTOCOL_PROPERTY);
        }
    }
}
