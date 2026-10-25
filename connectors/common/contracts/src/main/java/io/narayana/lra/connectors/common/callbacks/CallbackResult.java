package io.narayana.lra.connectors.common.callbacks;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class CallbackResult {
    public CallbackStatus status;
    public String body;
    /**
     * Coordinator-internal: it is never part of the wire or persisted JSON.
     */
    @JsonIgnore
    public CallbackAddress updatedStatusCallback;

    public CallbackResult() {
    }

    public CallbackResult(CallbackStatus status, String body, CallbackAddress updatedStatusCallback) {
        this.status = status;
        this.body = body;
        this.updatedStatusCallback = updatedStatusCallback;
    }

    public CallbackResult(CallbackStatus status, String body) {
        this(status, body, null);
    }

    public CallbackResult(CallbackStatus status) {
        this(status, null, null);
    }

    public CallbackStatus getStatus() {
        return status;
    }

    public String getBody() {
        return body;
    }

    public CallbackAddress getUpdatedStatusCallback() {
        return updatedStatusCallback;
    }
}
