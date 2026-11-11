/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.api;

/**
 * Thrown by the LRA interceptor when the LRA lifecycle associated with an intercepted method
 * completed with failed steps (for example a close, cancel or leave that did not succeed).
 * <p>
 * It is distinct from {@code LRAAbortException}: an abort means the invocation never ran (the
 * request processing stopped the method before it could execute), whereas a lifecycle failure
 * means the method ran but the surrounding LRA did not end cleanly.
 */
public class LifecycleException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public LifecycleException(String message) {
        super(message);
    }
}
