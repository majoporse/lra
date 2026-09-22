package io.narayana.lra.callbacks;

public class CallbackResult {
    public CallbackStatus status;
    public String body;
    public LRACallback updatedStatusCallback;

    public CallbackResult() {
    }

    public CallbackResult(CallbackStatus status, String body, LRACallback updatedStatusCallback) {
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

    public LRACallback getUpdatedStatusCallback() {
        return updatedStatusCallback;
    }
}
