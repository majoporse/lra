/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.coordinator;

import io.narayana.lra.connectors.common.callbacks.CallbackResult;
import io.narayana.lra.connectors.common.callbacks.CallbackStatus;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.contracts.CallbackOutcome;

/**
 * The one place a contract reply's outcome is interpreted for the LRA.
 */
public final class CallbackOutcomeMapper {

    private CallbackOutcomeMapper() {
    }

    public static CallbackResult map(CallbackType operation, CallbackOutcome reply) {
        return switch (operation) {
            case COMPENSATE, COMPLETE -> end(reply);
            case STATUS -> status(reply);
            case FORGET, AFTER -> ack(reply);
        };
    }

    public static CallbackResult end(CallbackOutcome reply) {
        if (reply.error() != null || reply.status() == null) {
            return new CallbackResult(CallbackStatus.ERROR, reply.error());
        }

        return switch (reply.status()) {
            case Completed, Compensated -> new CallbackResult(CallbackStatus.OK);
            case Completing, Compensating -> new CallbackResult(CallbackStatus.ACCEPTED);
            case FailedToComplete, FailedToCompensate ->
                new CallbackResult(CallbackStatus.FAILED, reply.status().name());
            default -> new CallbackResult(CallbackStatus.ERROR);
        };
    }

    public static CallbackResult status(CallbackOutcome reply) {
        if (reply.error() != null || reply.status() == null) {
            return new CallbackResult(CallbackStatus.ERROR, reply.error());
        }

        return switch (reply.status()) {
            case Completed, Compensated, FailedToComplete, FailedToCompensate ->
                new CallbackResult(CallbackStatus.OK, reply.status().name());
            default -> new CallbackResult(CallbackStatus.ACCEPTED);
        };
    }

    public static CallbackResult ack(CallbackOutcome reply) {
        return reply.error() == null ? new CallbackResult(CallbackStatus.OK)
                : new CallbackResult(CallbackStatus.ERROR, reply.error());
    }
}
