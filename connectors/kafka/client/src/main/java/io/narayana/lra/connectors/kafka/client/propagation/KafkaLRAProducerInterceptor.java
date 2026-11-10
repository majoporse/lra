/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.client.propagation;

import io.narayana.lra.Current;
import io.narayana.lra.connectors.kafka.support.LRAKafkaConstants;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Puts the effective LRA on every record sent through a producer this interceptor is installed on.
 * <p>
 * Enable it per outgoing channel with
 * {@code mp.messaging.outgoing.<channel>.interceptor.classes=io.narayana.lra.connectors.kafka.client.propagation.KafkaLRAProducerInterceptor}.
 * The header name and value match HTTP, so a consumer reads the same value from either transport. An
 * explicit header always wins and no header is written when there is no effective LRA or when
 * {@code mp.lra.propagation.active} is false.
 */
public class KafkaLRAProducerInterceptor implements ProducerInterceptor<Object, Object> {

    static final String PROPAGATION_ACTIVE = "mp.lra.propagation.active";

    private boolean canPropagate = true;

    public KafkaLRAProducerInterceptor() {
    }

    KafkaLRAProducerInterceptor(boolean canPropagate) {
        this.canPropagate = canPropagate;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        canPropagate = ConfigProvider.getConfig()
                .getOptionalValue(PROPAGATION_ACTIVE, Boolean.class).orElse(true);
    }

    @Override
    public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> record) {
        if (!canPropagate) {
            return record;
        }

        URI lraId = Current.peek();

        if (lraId != null && record.headers().lastHeader(LRAKafkaConstants.LRA_CONTEXT_HEADER) == null) {
            record.headers().add(LRAKafkaConstants.LRA_CONTEXT_HEADER,
                    lraId.toASCIIString().getBytes(StandardCharsets.UTF_8));
        }

        String token = Current.getAuthToken();

        if (token != null && record.headers().lastHeader(LRAKafkaConstants.AUTHORIZATION_HEADER) == null) {
            record.headers().add(LRAKafkaConstants.AUTHORIZATION_HEADER,
                    ("Bearer " + token).getBytes(StandardCharsets.UTF_8));
        }

        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // the record is already stamped; there is nothing to do on acknowledgement
    }

    @Override
    public void close() {
        // no resources are held
    }
}
