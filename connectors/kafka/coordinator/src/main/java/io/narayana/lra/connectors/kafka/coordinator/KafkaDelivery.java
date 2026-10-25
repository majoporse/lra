/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.coordinator;

import io.narayana.lra.api.LRA;
import io.narayana.lra.connectors.common.callbacks.CallbackContext;
import io.narayana.lra.connectors.common.callbacks.CallbackResult;
import io.narayana.lra.connectors.common.callbacks.CallbackStatus;
import io.narayana.lra.connectors.common.coordinator.CallbackDelivery;
import io.narayana.lra.connectors.common.coordinator.CallbackOutcomeMapper;
import io.narayana.lra.connectors.kafka.address.KafkaAddress;
import io.narayana.lra.connectors.kafka.contracts.AfterLRAKafkaCallback;
import io.narayana.lra.connectors.kafka.contracts.CompensateKafkaCallback;
import io.narayana.lra.connectors.kafka.contracts.CompleteKafkaCallback;
import io.narayana.lra.connectors.kafka.contracts.ForgetKafkaCallback;
import io.narayana.lra.connectors.kafka.contracts.KafkaCallbackReply;
import io.narayana.lra.connectors.kafka.contracts.KafkaRequest;
import io.narayana.lra.connectors.kafka.contracts.StatusKafkaCallback;
import io.narayana.lra.connectors.kafka.outbound.KafkaOutbound;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.time.Duration;
import java.util.UUID;

/**
 * Delivers a callback on the participant's Kafka service topic and maps the contract reply.
 */
@ApplicationScoped
public class KafkaDelivery implements CallbackDelivery<KafkaAddress> {

    @Inject
    KafkaOutbound kafkaOutbound;

    @Override
    public Class<KafkaAddress> addressType() {
        return KafkaAddress.class;
    }

    @Override
    public CallbackResult request(KafkaAddress address, CallbackContext context, Duration timeout) {
        String correlationId = UUID.randomUUID().toString();
        KafkaRequest<? extends KafkaCallbackReply> request = newRequest(address, context, correlationId,
                kafkaOutbound.coordinatorTopic());

        KafkaCallbackReply reply;

        try {
            reply = kafkaOutbound.request(address.topic(), request, timeout);
        } catch (RuntimeException e) {
            return new CallbackResult(CallbackStatus.ERROR,
                    e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }

        return reply == null ? new CallbackResult(CallbackStatus.TIMEOUT)
                : CallbackOutcomeMapper.map(address.operation(), reply);
    }

    private static KafkaRequest<? extends KafkaCallbackReply> newRequest(KafkaAddress address,
            CallbackContext context, String correlationId, String replyTopic) {
        URI lraUri = context.getLraId() == null ? null : URI.create(context.getLraId());
        URI parentUri = context.getParentId() == null ? null : URI.create(context.getParentId());
        String participantId = address.participantId();
        String recoveryUrl = context.getRecoveryId();
        String compensatorData = context.getCompensatorData();

        return switch (address.operation()) {
            case COMPENSATE -> new CompensateKafkaCallback.Request(correlationId, replyTopic, participantId,
                    lraUri, parentUri, recoveryUrl, compensatorData);
            case COMPLETE -> new CompleteKafkaCallback.Request(correlationId, replyTopic, participantId,
                    lraUri, parentUri, recoveryUrl, compensatorData);
            case STATUS -> new StatusKafkaCallback.Request(correlationId, replyTopic, participantId,
                    lraUri, parentUri, recoveryUrl, compensatorData);
            case FORGET -> new ForgetKafkaCallback.Request(correlationId, replyTopic, participantId,
                    lraUri, parentUri, recoveryUrl, compensatorData);
            case AFTER -> {
                String payload = context.getPayload();
                yield new AfterLRAKafkaCallback.Request(correlationId, replyTopic, participantId,
                        lraUri, parentUri, recoveryUrl, compensatorData,
                        payload == null ? null : LRA.TransactionStatus.valueOf(payload));
            }
        };
    }
}
