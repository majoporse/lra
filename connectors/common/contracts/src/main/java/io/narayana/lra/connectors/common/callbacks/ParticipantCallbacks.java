package io.narayana.lra.connectors.common.callbacks;

public class ParticipantCallbacks {

    public CallbackAddress compensateCallback;
    public CallbackAddress completeCallback;
    public CallbackAddress statusCallback;
    public CallbackAddress forgetCallback;
    public CallbackAddress afterCallback;

    public CallbackAddress callback(CallbackType type) {
        return switch (type) {
            case COMPENSATE -> compensateCallback;
            case COMPLETE -> completeCallback;
            case STATUS -> statusCallback;
            case FORGET -> forgetCallback;
            case AFTER -> afterCallback;
        };
    }

    public void callback(CallbackType type, CallbackAddress callback) {
        switch (type) {
            case COMPENSATE -> compensateCallback = callback;
            case COMPLETE -> completeCallback = callback;
            case STATUS -> statusCallback = callback;
            case FORGET -> forgetCallback = callback;
            case AFTER -> afterCallback = callback;
        }
    }

    public boolean hasAnyCallback() {
        return compensateCallback != null || completeCallback != null || statusCallback != null
                || forgetCallback != null || afterCallback != null;
    }

    public boolean hasCompensateOrComplete() {
        return compensateCallback != null || completeCallback != null;
    }
}
