/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.api;

/**
 * Transport-neutral classification of the result of an intercepted method invocation, used to
 * decide whether the associated LRA should be closed or cancelled.
 */
public enum Outcome {
    /** the invocation completed successfully (the HTTP 2xx family) */
    SUCCESS,

    /** a provisional response (the HTTP 1xx family) */
    INFORMATIONAL,

    /** a redirection (the HTTP 3xx family) */
    REDIRECTION,

    /** the invocation failed because of the caller (equivalent to the HTTP 4xx family) */
    CLIENT_ERROR,

    /** the invocation failed because of the service (equivalent to the HTTP 5xx family) */
    SERVER_ERROR,

    /** a family the transport does not classify */
    OTHER
}
