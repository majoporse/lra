/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.client.propagation;

import io.narayana.lra.Current;
import io.narayana.lra.LRAConstants;
import io.narayana.lra.connectors.kafka.support.LRAKafkaConstants;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KafkaLRAProducerInterceptorTest {

    private URI lraId;

    @BeforeEach
    public void setUp() {
        Current.popAll();
        lraId = LRAConstants.toLRAId(UUID.randomUUID());
    }

    @AfterEach
    public void tearDown() {
        Current.popAll();
        System.clearProperty(KafkaLRAProducerInterceptor.PROPAGATION_ACTIVE);
    }

    @Test
    public void stampsTheEffectiveLra() {
        activate(lraId);

        try {
            ProducerRecord<Object, Object> record = new ProducerRecord<>("orders", "payload");

            new KafkaLRAProducerInterceptor(true).onSend(record);

            Assertions.assertEquals(lraId.toASCIIString(), headerValue(record));
        } finally {
            Current.removeActiveLRACache(lraId);
        }
    }

    @Test
    public void leavesAnExplicitHeaderAlone() {
        activate(lraId);

        try {
            ProducerRecord<Object, Object> record = new ProducerRecord<>("orders", "payload");
            record.headers().add(LRAKafkaConstants.LRA_CONTEXT_HEADER,
                    "explicit".getBytes(StandardCharsets.UTF_8));

            new KafkaLRAProducerInterceptor(true).onSend(record);

            Assertions.assertEquals("explicit", headerValue(record));
            Assertions.assertEquals(1, record.headers().toArray().length);
        } finally {
            Current.removeActiveLRACache(lraId);
        }
    }

    @Test
    public void writesNothingWithoutAnLra() {
        ProducerRecord<Object, Object> record = new ProducerRecord<>("orders", "payload");

        new KafkaLRAProducerInterceptor(true).onSend(record);

        Assertions.assertNull(record.headers().lastHeader(LRAKafkaConstants.LRA_CONTEXT_HEADER));
    }

    @Test
    public void writesNothingWhenPropagationIsDisabled() {
        activate(lraId);

        try {
            ProducerRecord<Object, Object> record = new ProducerRecord<>("orders", "payload");

            new KafkaLRAProducerInterceptor(false).onSend(record);

            Assertions.assertNull(record.headers().lastHeader(LRAKafkaConstants.LRA_CONTEXT_HEADER));
        } finally {
            Current.removeActiveLRACache(lraId);
        }
    }

    @Test
    public void readsThePropagationSetting() {
        System.setProperty(KafkaLRAProducerInterceptor.PROPAGATION_ACTIVE, "false");

        KafkaLRAProducerInterceptor interceptor = new KafkaLRAProducerInterceptor();
        interceptor.configure(Map.of());

        activate(lraId);

        try {
            ProducerRecord<Object, Object> record = new ProducerRecord<>("orders", "payload");

            interceptor.onSend(record);

            Assertions.assertNull(record.headers().lastHeader(LRAKafkaConstants.LRA_CONTEXT_HEADER));
        } finally {
            Current.removeActiveLRACache(lraId);
        }
    }

    private static void activate(URI lraId) {
        Current.push(lraId, null);
        Current.addActiveLRACache(lraId);
    }

    private static String headerValue(ProducerRecord<Object, Object> record) {
        return new String(record.headers().lastHeader(LRAKafkaConstants.LRA_CONTEXT_HEADER).value(),
                StandardCharsets.UTF_8);
    }
}
