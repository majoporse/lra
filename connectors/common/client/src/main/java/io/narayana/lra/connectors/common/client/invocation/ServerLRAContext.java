/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.invocation;

import io.narayana.lra.api.Outcome;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;

/**
 * Holds the state that is produced and consumed while processing one LRA-managed invocation.
 * <p>
 * It is transport-neutral: the cancel conditions are {@link Outcome families} and numeric codes,
 * never Jakarta REST types. A transport adapter fills it from its own request/message and completes
 * it from its own response.
 */
public class ServerLRAContext {

    private final Method method;
    private final Class<?> resourceClass;

    private URI incomingLRA;

    private LRA.Type type;
    private boolean endLRA;
    private Long timeout;
    private Set<Integer> cancelOn;
    private Set<Outcome> cancelOnFamily;
    private boolean endAnnotation;
    private boolean requiresActiveLRA;

    private URI currentLRA;
    private boolean suspendedLRA;
    private URI parentLRA;
    private boolean parentDecided;
    private boolean clearIncomingLRA;

    private ParticipantCallbacks callbacks;
    private String recoveryUrl;
    private String participantId;
    private String userData;

    private List<Progress> progress;

    private boolean aborted;
    private int abortStatusCode;
    private String abortMessage;

    private boolean currentLRACancelled;
    private boolean endLRAClosed;

    public ServerLRAContext(Method method, Class<?> resourceClass) {
        this.method = method;
        this.resourceClass = resourceClass;
    }

    public Method getMethod() {
        return method;
    }

    public Class<?> getResourceClass() {
        return resourceClass;
    }

    public URI getIncomingLRA() {
        return incomingLRA;
    }

    public void setIncomingLRA(URI incomingLRA) {
        this.incomingLRA = incomingLRA;
    }

    public LRA.Type getType() {
        return type;
    }

    public void setType(LRA.Type type) {
        this.type = type;
    }

    public boolean isEndLRA() {
        return endLRA;
    }

    public void setEndLRA(boolean endLRA) {
        this.endLRA = endLRA;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public Set<Integer> getCancelOn() {
        return cancelOn;
    }

    public void setCancelOn(Set<Integer> cancelOn) {
        this.cancelOn = cancelOn;
    }

    public Set<Outcome> getCancelOnFamily() {
        return cancelOnFamily;
    }

    public void setCancelOnFamily(Set<Outcome> cancelOnFamily) {
        this.cancelOnFamily = cancelOnFamily;
    }

    public boolean isEndAnnotation() {
        return endAnnotation;
    }

    public void setEndAnnotation(boolean endAnnotation) {
        this.endAnnotation = endAnnotation;
    }

    public boolean isRequiresActiveLRA() {
        return requiresActiveLRA;
    }

    public void setRequiresActiveLRA(boolean requiresActiveLRA) {
        this.requiresActiveLRA = requiresActiveLRA;
    }

    public URI getCurrentLRA() {
        return currentLRA;
    }

    public void setCurrentLRA(URI currentLRA) {
        this.currentLRA = currentLRA;
    }

    public boolean isSuspendedLRA() {
        return suspendedLRA;
    }

    public void setSuspendedLRA(boolean suspendedLRA) {
        this.suspendedLRA = suspendedLRA;
    }

    public URI getParentLRA() {
        return parentLRA;
    }

    public void setParentLRA(URI parentLRA) {
        this.parentLRA = parentLRA;
    }

    public boolean isParentDecided() {
        return parentDecided;
    }

    public void setParentDecided(boolean parentDecided) {
        this.parentDecided = parentDecided;
    }

    public boolean isClearIncomingLRA() {
        return clearIncomingLRA;
    }

    public void setClearIncomingLRA(boolean clearIncomingLRA) {
        this.clearIncomingLRA = clearIncomingLRA;
    }

    public ParticipantCallbacks getCallbacks() {
        return callbacks;
    }

    public void setCallbacks(ParticipantCallbacks callbacks) {
        this.callbacks = callbacks;
    }

    public String getRecoveryUrl() {
        return recoveryUrl;
    }

    public void setRecoveryUrl(String recoveryUrl) {
        this.recoveryUrl = recoveryUrl;
    }

    public String getParticipantId() {
        return participantId;
    }

    public void setParticipantId(String participantId) {
        this.participantId = participantId;
    }

    public String getUserData() {
        return userData;
    }

    public void setUserData(String userData) {
        this.userData = userData;
    }

    public List<Progress> getProgress() {
        return progress;
    }

    public void setProgress(List<Progress> progress) {
        this.progress = progress;
    }

    public void addProgress(ProgressStep step, String reason) {
        if (progress == null) {
            progress = new ArrayList<>();
        }
        progress.add(new Progress(step, reason));
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

    public boolean isCurrentLRACancelled() {
        return currentLRACancelled;
    }

    public void setCurrentLRACancelled(boolean currentLRACancelled) {
        this.currentLRACancelled = currentLRACancelled;
    }

    public boolean isEndLRAClosed() {
        return endLRAClosed;
    }

    public void setEndLRAClosed(boolean endLRAClosed) {
        this.endLRAClosed = endLRAClosed;
    }

    // the request filter may perform multiple steps and in failure scenarios the LRA may be left in an
    // ambiguous state: the following structure is used to track progress so that such failures can be
    // reported in the response filter processing
    public enum ProgressStep {
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

    // list of steps (both successful and unsuccessful) performed so far by the request and response filter
    // and is used for error reporting
    public static class Progress {
        public ProgressStep progress;
        public String reason;

        public Progress(ProgressStep progress, String reason) {
            this.progress = progress;
            this.reason = reason;
        }
    }
}
