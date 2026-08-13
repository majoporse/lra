/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

/**
 * Model for registering LRA participant callbacks.
 * Used when a messaging-based participant joins an LRA.
 */
public class RegistrationRequest {

    private String compensateEndpoint;
    private String completeEndpoint;
    private String statusEndpoint;
    private String forgetEndpoint;
    private String afterEndpoint;

    public RegistrationRequest() {
    }

    public String getCompensateEndpoint() {
        return compensateEndpoint;
    }

    public void setCompensateEndpoint(String compensateEndpoint) {
        this.compensateEndpoint = compensateEndpoint;
    }

    public String getCompleteEndpoint() {
        return completeEndpoint;
    }

    public void setCompleteEndpoint(String completeEndpoint) {
        this.completeEndpoint = completeEndpoint;
    }

    public String getStatusEndpoint() {
        return statusEndpoint;
    }

    public void setStatusEndpoint(String statusEndpoint) {
        this.statusEndpoint = statusEndpoint;
    }

    public String getForgetEndpoint() {
        return forgetEndpoint;
    }

    public void setForgetEndpoint(String forgetEndpoint) {
        this.forgetEndpoint = forgetEndpoint;
    }

    public String getAfterEndpoint() {
        return afterEndpoint;
    }

    public void setAfterEndpoint(String afterEndpoint) {
        this.afterEndpoint = afterEndpoint;
    }
}
