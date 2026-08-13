/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.filter;

import java.net.URI;
import java.util.List;

/**
 * Transport-agnostic response context for LRA processing.
 * Mirrors the properties read from {@code ContainerRequestContext} by {@link ServerLRAFilter}'s response filter.
 */
public class LRAResponseContext {

    // --- Input fields (set by the transport layer before calling the handler) ---

    private int responseStatus;
    private URI suspendedLRA;
    private URI currentLRA;
    private URI toClose;
    private boolean isCancel;
    private String userData;
    private String compensatorLink;
    private String failureMessage;

    // --- Flags set by the handler to indicate which operations were attempted ---

    // Set when the cancel finally block ran (current != null && isCancel)
    private boolean cancelledCurrent;
    // Set when the toClose finally block ran (toClose != null)
    private boolean closedTerminal;

    // The LRA context stack to write to response headers (captured before popAll)
    private List<Object> lraContexts;

    // --- Getters and setters ---

    public int getResponseStatus() {
        return responseStatus;
    }

    public void setResponseStatus(int responseStatus) {
        this.responseStatus = responseStatus;
    }

    public URI getSuspendedLRA() {
        return suspendedLRA;
    }

    public void setSuspendedLRA(URI suspendedLRA) {
        this.suspendedLRA = suspendedLRA;
    }

    public URI getCurrentLRA() {
        return currentLRA;
    }

    public void setCurrentLRA(URI currentLRA) {
        this.currentLRA = currentLRA;
    }

    public URI getToClose() {
        return toClose;
    }

    public void setToClose(URI toClose) {
        this.toClose = toClose;
    }

    public boolean isCancel() {
        return isCancel;
    }

    public void setCancel(boolean cancel) {
        isCancel = cancel;
    }

    public String getUserData() {
        return userData;
    }

    public void setUserData(String userData) {
        this.userData = userData;
    }

    public String getCompensatorLink() {
        return compensatorLink;
    }

    public void setCompensatorLink(String compensatorLink) {
        this.compensatorLink = compensatorLink;
    }

    public String getFailureMessage() {
        return failureMessage;
    }

    public void setFailureMessage(String failureMessage) {
        this.failureMessage = failureMessage;
    }

    public boolean isCancelledCurrent() {
        return cancelledCurrent;
    }

    public void setCancelledCurrent(boolean cancelledCurrent) {
        this.cancelledCurrent = cancelledCurrent;
    }

    public boolean isClosedTerminal() {
        return closedTerminal;
    }

    public void setClosedTerminal(boolean closedTerminal) {
        this.closedTerminal = closedTerminal;
    }

    public List<Object> getLraContexts() {
        return lraContexts;
    }

    public void setLraContexts(List<Object> lraContexts) {
        this.lraContexts = lraContexts;
    }
}
