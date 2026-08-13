/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.filter;

import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.Method;
import java.net.URI;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;

/**
 * Transport-agnostic request context for LRA processing.
 * Mirrors the properties stored on {@code ContainerRequestContext} by {@link ServerLRAFilter}.
 */
public class LRARequestContext {

    // --- Input fields (set by the transport layer before calling the handler) ---

    private URI incomingLRA;
    private Class<?> resourceClass;
    private Method resourceMethod;
    private URI baseUri;
    private URI requestBaseUri; // the actual request base URI (uriInfo.getBaseUri())
    private String authToken;
    private String authorizationHeader;
    private MultivaluedMap<String, String> headers;

    // --- Properties that mirror ServerLRAFilter's ContainerRequestContext properties ---

    private Response.Status.Family[] cancelOnFamily;
    private Response.Status[] cancelOn;
    private URI terminalLRA;
    private URI suspendedLRA;
    private URI currentLRA;
    private URI newLRA;
    private Object abortWith; // stored as Object like ServerLRAFilter
    private ProgressTracker progressTracker;
    private String compensatorLink;

    // --- LRA annotation properties (resolved by the handler) ---

    private LRA.Type type;
    private boolean isLongRunning;
    private Long timeout;
    private boolean endAnnotation;

    // --- Result fields (set by the handler) ---

    private boolean aborted;
    private int abortStatusCode;
    private String abortMessage;
    private URI recoveryUrl;
    private String parentContextHeader;
    private String previousParticipantData;

    // --- Flags for the transport layer ---

    // Set when the handler would have called Current.clearContext(headers).
    // The transport layer should remove the LRA_HTTP_CONTEXT_HEADER from request headers.
    private boolean clearContextHeaders;

    // --- Getters and setters ---

    public URI getIncomingLRA() {
        return incomingLRA;
    }

    public void setIncomingLRA(URI incomingLRA) {
        this.incomingLRA = incomingLRA;
    }

    public Class<?> getResourceClass() {
        return resourceClass;
    }

    public void setResourceClass(Class<?> resourceClass) {
        this.resourceClass = resourceClass;
    }

    public Method getResourceMethod() {
        return resourceMethod;
    }

    public void setResourceMethod(Method resourceMethod) {
        this.resourceMethod = resourceMethod;
    }

    public URI getBaseUri() {
        return baseUri;
    }

    public void setBaseUri(URI baseUri) {
        this.baseUri = baseUri;
    }

    public URI getRequestBaseUri() {
        return requestBaseUri;
    }

    public void setRequestBaseUri(URI requestBaseUri) {
        this.requestBaseUri = requestBaseUri;
    }

    public String getAuthToken() {
        return authToken;
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    public String getAuthorizationHeader() {
        return authorizationHeader;
    }

    public void setAuthorizationHeader(String authorizationHeader) {
        this.authorizationHeader = authorizationHeader;
    }

    public MultivaluedMap<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(MultivaluedMap<String, String> headers) {
        this.headers = headers;
    }

    public Response.Status.Family[] getCancelOnFamily() {
        return cancelOnFamily;
    }

    public void setCancelOnFamily(Response.Status.Family[] cancelOnFamily) {
        this.cancelOnFamily = cancelOnFamily;
    }

    public Response.Status[] getCancelOn() {
        return cancelOn;
    }

    public void setCancelOn(Response.Status[] cancelOn) {
        this.cancelOn = cancelOn;
    }

    public URI getTerminalLRA() {
        return terminalLRA;
    }

    public void setTerminalLRA(URI terminalLRA) {
        this.terminalLRA = terminalLRA;
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

    public URI getNewLRA() {
        return newLRA;
    }

    public void setNewLRA(URI newLRA) {
        this.newLRA = newLRA;
    }

    public Object getAbortWith() {
        return abortWith;
    }

    public void setAbortWith(Object abortWith) {
        this.abortWith = abortWith;
    }

    public ProgressTracker getProgressTracker() {
        return progressTracker;
    }

    public void setProgressTracker(ProgressTracker progressTracker) {
        this.progressTracker = progressTracker;
    }

    /**
     * Convenience method: add a progress step, lazily creating the tracker if needed.
     */
    public void setProgress(ProgressTracker.ProgressStep step, String reason) {
        if (progressTracker == null) {
            progressTracker = new ProgressTracker();
        }
        progressTracker.add(step, reason);
    }

    public String getCompensatorLink() {
        return compensatorLink;
    }

    public void setCompensatorLink(String compensatorLink) {
        this.compensatorLink = compensatorLink;
    }

    public LRA.Type getType() {
        return type;
    }

    public void setType(LRA.Type type) {
        this.type = type;
    }

    public boolean isLongRunning() {
        return isLongRunning;
    }

    public void setLongRunning(boolean longRunning) {
        isLongRunning = longRunning;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public boolean isEndAnnotation() {
        return endAnnotation;
    }

    public void setEndAnnotation(boolean endAnnotation) {
        this.endAnnotation = endAnnotation;
    }

    public boolean isAborted() {
        return aborted;
    }

    public void setAborted(boolean aborted) {
        this.aborted = aborted;
    }

    public int getAbortStatusCode() {
        return abortStatusCode;
    }

    public void setAbortStatusCode(int abortStatusCode) {
        this.abortStatusCode = abortStatusCode;
    }

    public String getAbortMessage() {
        return abortMessage;
    }

    public void setAbortMessage(String abortMessage) {
        this.abortMessage = abortMessage;
    }

    public URI getRecoveryUrl() {
        return recoveryUrl;
    }

    public void setRecoveryUrl(URI recoveryUrl) {
        this.recoveryUrl = recoveryUrl;
    }

    public String getParentContextHeader() {
        return parentContextHeader;
    }

    public void setParentContextHeader(String parentContextHeader) {
        this.parentContextHeader = parentContextHeader;
    }

    public String getPreviousParticipantData() {
        return previousParticipantData;
    }

    public void setPreviousParticipantData(String previousParticipantData) {
        this.previousParticipantData = previousParticipantData;
    }

    public boolean isClearContextHeaders() {
        return clearContextHeaders;
    }

    public void setClearContextHeaders(boolean clearContextHeaders) {
        this.clearContextHeaders = clearContextHeaders;
    }
}
