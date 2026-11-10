/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.invocation;

import io.narayana.lra.api.Outcome;

/**
 * The transport-neutral result of an intercepted invocation, used to decide whether the LRA is
 * closed or cancelled.
 * <p>
 * The exact status code is optional. It is present when the transport has one (an HTTP participant
 * method) and is used for {@code cancelOn} exact-code rules; transports without a status use the
 * outcome family and {@code cancelOnFamily} only.
 *
 * @param outcome the outcome family
 * @param statusCode the transport status code, or {@code null} when the transport has none
 * @param message a human-readable failure message, or {@code null}
 * @param failed whether the invocation is considered a failure for methods without an {@code @LRA}
 */
public record LraCompletion(Outcome outcome, Integer statusCode, String message, boolean failed) {

    public static LraCompletion success() {
        return new LraCompletion(Outcome.SUCCESS, null, null, false);
    }

    public static LraCompletion failure(String message) {
        return new LraCompletion(Outcome.SERVER_ERROR, null, message, true);
    }

    public static LraCompletion ofStatus(int statusCode, String message) {
        return new LraCompletion(outcomeOf(statusCode), statusCode, message, statusCode >= 400);
    }

    public static Outcome outcomeOf(int statusCode) {
        if (statusCode >= 100 && statusCode < 200) {
            return Outcome.INFORMATIONAL;
        }

        if (statusCode >= 200 && statusCode < 300) {
            return Outcome.SUCCESS;
        }

        if (statusCode >= 300 && statusCode < 400) {
            return Outcome.REDIRECTION;
        }

        if (statusCode >= 400 && statusCode < 500) {
            return Outcome.CLIENT_ERROR;
        }

        if (statusCode >= 500 && statusCode < 600) {
            return Outcome.SERVER_ERROR;
        }

        return Outcome.OTHER;
    }
}
