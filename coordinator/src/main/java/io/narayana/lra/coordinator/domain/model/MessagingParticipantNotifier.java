/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.domain.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.logging.LRALogger;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

/**
 * Messaging-based participant notifier. Sends LRA callbacks as messages
 * to topics instead of making direct HTTP calls.
 *
 * <p>
 * The {@code targetEndpoint} uses the {@code kafka://} scheme.
 * The topic name is extracted from the endpoint (e.g., {@code kafka://lra-compensate}
 * → topic {@code lra-compensate}).
 * </p>
 *
 * <p>
 * Status queries are not supported (return null) — they remain HTTP-only
 * via the recovery module.
 * </p>
 *
 * <p>
 * Note: This notifier requires a {@code MessagingSender} to be set
 * via {@link #setSender(MessagingSender)} before use. The sender is
 * provided by the Quarkus deployment module (e.g., SmallRye Reactive Messaging).
 * </p>
 */
public class MessagingParticipantNotifier implements ParticipantNotifier {

    private static final String KAFKA_SCHEME = "kafka://";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private volatile MessagingSender sender;

    /**
     * Interface for sending messages to a topic.
     * Implemented by the Quarkus deployment module using SmallRye Reactive Messaging.
     */
    public interface MessagingSender {
        void send(String topic, String key, String payload);
    }

    public void setSender(MessagingSender sender) {
        this.sender = sender;
    }

    @Override
    public boolean supports(String endpoint) {
        return endpoint != null && endpoint.startsWith(KAFKA_SCHEME);
    }

    @Override
    public int notifyCompensate(String lraId, String parentId, String targetEndpoint,
            String recoveryURI, String compensatorData) {
        return sendCallback(lraId, parentId, targetEndpoint, recoveryURI, compensatorData, "COMPENSATE", null);
    }

    @Override
    public int notifyComplete(String lraId, String parentId, String targetEndpoint,
            String recoveryURI, String compensatorData) {
        return sendCallback(lraId, parentId, targetEndpoint, recoveryURI, compensatorData, "COMPLETE", null);
    }

    @Override
    public boolean notifyForget(String lraId, String parentId, String targetEndpoint,
            String recoveryURI, String compensatorData) {
        return sendCallback(lraId, parentId, targetEndpoint, recoveryURI, compensatorData, "FORGET", null) != -2;
    }

    @Override
    public boolean notifyAfterLRA(String lraId, String parentId, String targetEndpoint,
            String recoveryURI, String compensatorData, String payload) {
        return sendCallback(lraId, parentId, targetEndpoint, recoveryURI, compensatorData, "AFTER_LRA", payload) != -2;
    }

    @Override
    public ParticipantStatus queryStatus(String lraId, String statusEndpoint,
            String recoveryURI, String compensatorData) {
        // Status queries remain HTTP-only
        return null;
    }

    private int sendCallback(String lraId, String parentId, String targetEndpoint,
            String recoveryURI, String compensatorData, String callbackType, String afterLRAPayload) {
        if (sender == null) {
            LRALogger.logger.errorf("MessagingParticipantNotifier: no sender configured, cannot deliver %s callback for LRA %s",
                    callbackType, lraId);
            return -2;
        }

        String topic = extractTopic(targetEndpoint);
        if (topic == null) {
            LRALogger.logger.errorf("MessagingParticipantNotifier: cannot extract topic from endpoint %s", targetEndpoint);
            return -2;
        }

        try {
            CallbackPayload payload = new CallbackPayload(
                    lraId, parentId, targetEndpoint,
                    recoveryURI, compensatorData,
                    callbackType, afterLRAPayload);
            String json = objectMapper.writeValueAsString(payload);
            String key = lraId != null ? lraId : "unknown";

            sender.send(topic, key, json);

            LRALogger.logger.debugf("MessagingParticipantNotifier: sent %s callback to topic %s for LRA %s",
                    callbackType, topic, lraId);

            return -1; // async delivery
        } catch (JsonProcessingException e) {
            LRALogger.logger.errorf("MessagingParticipantNotifier: failed to serialize callback payload for LRA %s: %s",
                    lraId, e.getMessage());
            return -2;
        }
    }

    /**
     * Extract the topic name from a kafka:// endpoint.
     * Examples:
     * "kafka://lra-compensate" → "lra-compensate"
     * "kafka://my-topic" → "my-topic"
     */
    static String extractTopic(String endpoint) {
        if (endpoint == null || !endpoint.startsWith(KAFKA_SCHEME)) {
            return null;
        }
        return endpoint.substring(KAFKA_SCHEME.length());
    }
}
