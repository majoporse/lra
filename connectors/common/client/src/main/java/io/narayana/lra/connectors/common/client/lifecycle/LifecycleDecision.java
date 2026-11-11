/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.lifecycle;

/**
 * What the lifecycle processor decided for one invocation.
 *
 * @param aborted whether the invocation was stopped before the business method ran
 * @param abortStatusCode the status a transport should report for the abort
 * @param abortMessage the reason the invocation was aborted
 * @param failureMessage a lifecycle failure to surface to the caller, or {@code null}
 */
public record LifecycleDecision(
        boolean aborted,
        int abortStatusCode,
        String abortMessage,
        String failureMessage) {

    public boolean hasFailure() {
        return failureMessage != null;
    }
}
