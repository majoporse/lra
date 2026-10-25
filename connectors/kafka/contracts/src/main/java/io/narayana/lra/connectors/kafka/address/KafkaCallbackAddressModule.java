/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.address;

import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * Registers the Kafka callback address subtype with Jackson.
 * <p>
 * {@code ObjectMapper.findAndRegisterModules()} discovers this module through
 * {@code META-INF/services/com.fasterxml.jackson.databind.Module}, so the coordinator and the
 * participant can read Kafka addresses without a central subtype list.
 */
public final class KafkaCallbackAddressModule extends SimpleModule {

    public KafkaCallbackAddressModule() {
        super("lra-kafka-callback-address");
        registerSubtypes(KafkaAddress.class);
    }
}
