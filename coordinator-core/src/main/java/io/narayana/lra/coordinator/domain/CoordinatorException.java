/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.domain;

/**
 * A coordinator operation failure that is independent of HTTP and Kafka.
 */
public class CoordinatorException extends RuntimeException {

    public enum Reason {
        NOT_FOUND,
        INVALID_STATE,
        GONE,
        INVALID_REQUEST,
        UNAVAILABLE,
        INTERNAL
    }

    private final Reason reason;

    public CoordinatorException(Reason reason, String message) {
        this(reason, message, null);
    }

    public CoordinatorException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}
