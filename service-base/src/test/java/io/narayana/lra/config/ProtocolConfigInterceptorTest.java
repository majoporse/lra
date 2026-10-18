/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.config;

import io.narayana.lra.callbacks.CallbackFactory;
import io.narayana.lra.callbacks.Protocol;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProtocolConfigInterceptorTest {

    @Test
    public void derivesTrueWhenKafka() {
        System.setProperty(CallbackFactory.PROTOCOL_PROPERTY, Protocol.KAFKA.value());
        try {
            Assertions.assertTrue(
                    ConfigProvider.getConfig().getValue(ProtocolConfigInterceptor.KAFKA_PROPERTY, Boolean.class));
        } finally {
            System.clearProperty(CallbackFactory.PROTOCOL_PROPERTY);
        }
    }

    @Test
    public void derivesFalseForHttp() {
        System.setProperty(CallbackFactory.PROTOCOL_PROPERTY, Protocol.HTTP.value());
        try {
            Assertions.assertFalse(
                    ConfigProvider.getConfig().getValue(ProtocolConfigInterceptor.KAFKA_PROPERTY, Boolean.class));
        } finally {
            System.clearProperty(CallbackFactory.PROTOCOL_PROPERTY);
        }
    }

    @Test
    public void derivesFalseWhenUnset() {
        System.clearProperty(CallbackFactory.PROTOCOL_PROPERTY);
        Assertions.assertFalse(
                ConfigProvider.getConfig().getValue(ProtocolConfigInterceptor.KAFKA_PROPERTY, Boolean.class));
    }

    @Test
    public void rejectsUnknownProtocol() {
        System.setProperty(CallbackFactory.PROTOCOL_PROPERTY, "carrier-pigeon");
        try {
            Assertions.assertThrows(IllegalArgumentException.class, () -> ConfigProvider.getConfig()
                    .getValue(ProtocolConfigInterceptor.KAFKA_PROPERTY, Boolean.class));
        } finally {
            System.clearProperty(CallbackFactory.PROTOCOL_PROPERTY);
        }
    }
}
