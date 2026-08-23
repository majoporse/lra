package io.narayana.lra.coordinator.domain;

public class LRAException extends RuntimeException {
    public enum Type {
        NOT_FOUND,
        PRECONDITION_FAILED,
        SERVICE_UNAVAILABLE,
        BAD_REQUEST,
        INTERNAL_SERVER_ERROR
    }

    private final Type type;

    public LRAException(String message, Type type) {
        super(message);
        this.type = type;
    }

    public LRAException(String message, Throwable cause, Type type) {
        super(message, cause);
        this.type = type;
    }

    public Type getType() {
        return type;
    }
}