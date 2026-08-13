/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.filter;

import io.narayana.lra.logging.LRALogger;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.StringJoiner;

/**
 * Tracks progress of LRA operations (leave, start, join, close, cancel)
 * so that failures can be reported in the response filter.
 *
 * <p>
 * The request filter may perform multiple operations, and in failure scenarios
 * the LRA may be left in an ambiguous state. This tracker records each step
 * (both successful and unsuccessful) so the response filter can report them.
 * </p>
 */
class ProgressTracker {

    enum ProgressStep {
        Left("leave succeeded"),
        LeaveFailed("leave failed"),
        Started("start succeeded"),
        StartFailed("start failed"),
        Joined("join succeeded"),
        JoinFailed("join failed"),
        Ended("end succeeded"),
        CloseFailed("close failed"),
        CancelFailed("cancel failed");

        final String status;

        ProgressStep(final String status) {
            this.status = status;
        }

        @Override
        public String toString() {
            return status;
        }
    }

    private static class Progress {
        private static final EnumSet<ProgressStep> FAILURES = EnumSet.of(
                ProgressStep.LeaveFailed,
                ProgressStep.StartFailed,
                ProgressStep.JoinFailed,
                ProgressStep.CloseFailed,
                ProgressStep.CancelFailed);

        final ProgressStep step;
        final String reason;

        Progress(ProgressStep step, String reason) {
            this.step = step;
            this.reason = reason;
        }

        boolean wasSuccessful() {
            return !FAILURES.contains(step);
        }
    }

    private final ArrayList<Progress> steps = new ArrayList<>();

    void add(ProgressStep step, String reason) {
        if (reason == null) {
            LRALogger.logger.debug(step.toString());
        } else {
            steps.add(new Progress(step, reason));
        }
    }

    boolean doesNotContain(ProgressStep step) {
        return steps.stream().noneMatch(p -> p.step == step);
    }

    boolean isEmpty() {
        return steps.isEmpty();
    }

    /**
     * Build a warning message for any failed steps.
     *
     * @return the failure message, or null if all steps succeeded
     */
    String buildFailureMessage() {
        if (steps.isEmpty()) {
            return null;
        }

        StringJoiner badOps = new StringJoiner(", ");
        StringBuilder code = new StringBuilder("-");

        steps.forEach(p -> {
            code.append(p.step.ordinal());
            badOps.add(String.format("%s (%s)", p.step.name(), p.reason));
        });

        if (badOps.length() != 0) {
            return LRALogger.i18nLogger.warn_LRAStatusInDoubt(String.format("%s: %s", code, badOps));
        }

        return null;
    }

    /**
     * Cast the tracker back from an Object stored in the request context.
     * Returns null if the object is not a ProgressTracker.
     */
    @SuppressWarnings("unchecked")
    static ProgressTracker from(Object obj) {
        if (obj instanceof ProgressTracker) {
            return (ProgressTracker) obj;
        }
        return null;
    }
}
