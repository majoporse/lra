/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.domain.model;

/**
 * JSON-serializable payload for LRA participant callbacks sent via messaging.
 * Used by {@link MessagingParticipantNotifier} to serialize callback data
 * and by KafkaCallbackConsumer to deserialize it.
 */
public class CallbackPayload {
    private String lraId;
    private String parentId;
    private String targetEndpoint; // "http://..." or "kafka://..."
    private String recoveryURI;
    private String compensatorData;
    private String callbackType; // COMPENSATE, COMPLETE, FORGET, AFTER_LRA
    private String afterLRAPayload; // LRAStatus name, only for AFTER_LRA
    private long timestamp;

    public CallbackPayload() {
    }

    public CallbackPayload(String lraId, String parentId, String targetEndpoint,
            String recoveryURI, String compensatorData,
            String callbackType, String afterLRAPayload) {
        this.lraId = lraId;
        this.parentId = parentId;
        this.targetEndpoint = targetEndpoint;
        this.recoveryURI = recoveryURI;
        this.compensatorData = compensatorData;
        this.callbackType = callbackType;
        this.afterLRAPayload = afterLRAPayload;
        this.timestamp = System.currentTimeMillis();
    }

    public String getLraId() {
        return lraId;
    }

    public void setLraId(String lraId) {
        this.lraId = lraId;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public String getTargetEndpoint() {
        return targetEndpoint;
    }

    public void setTargetEndpoint(String targetEndpoint) {
        this.targetEndpoint = targetEndpoint;
    }

    public String getRecoveryURI() {
        return recoveryURI;
    }

    public void setRecoveryURI(String recoveryURI) {
        this.recoveryURI = recoveryURI;
    }

    public String getCompensatorData() {
        return compensatorData;
    }

    public void setCompensatorData(String compensatorData) {
        this.compensatorData = compensatorData;
    }

    public String getCallbackType() {
        return callbackType;
    }

    public void setCallbackType(String callbackType) {
        this.callbackType = callbackType;
    }

    public String getAfterLRAPayload() {
        return afterLRAPayload;
    }

    public void setAfterLRAPayload(String afterLRAPayload) {
        this.afterLRAPayload = afterLRAPayload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
