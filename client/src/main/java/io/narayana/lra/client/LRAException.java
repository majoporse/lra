/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

/**
 * Transport-agnostic exception for LRA coordinator communication failures.
 */
public class LRAException extends Exception {

    private int status;

    public LRAException(String message) {
        super(message);
    }

    public LRAException(String message, Throwable cause) {
        super(message, cause);
    }

    public LRAException(int status, String message) {
        super(message);
        this.status = status;
    }

    public LRAException(int status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    public int getStatus() {
        return status;
    }
}
