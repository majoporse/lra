/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.invocation;

import io.narayana.lra.Current;
import io.narayana.lra.api.Outcome;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;

/**
 * The state produced and consumed while processing one LRA-managed invocation.
 * <p>
 * It is transport-neutral: the cancel conditions are {@link Outcome families} and numeric codes,
 * never Jakarta REST types. A transport adapter fills it from its own request/message and completes
 * it from its own response.
 * <p>
 * The same object is the handoff to the transport response adapter: the interceptor stores it on
 * the request, and the adapter completes the lifecycle once the response exists. An asynchronous
 * invocation may be completed from another thread, so completion reads only this object and never
 * the calling thread's {@code Current}. It keeps the thread-local state in effect before the
 * invocation so that completion restores it.
 */
public final class LraInvocationState {

    /**
     * Request property under which the interceptor stores the invocation state.
     */
    public static final String PROPERTY = LraInvocationState.class.getName();

    private final Method method;
    private final Class<?> resourceClass;
    private final LraMethodMetadata metadata;
    private final List<Object> contextsOnEntry;
    private final String authTokenOnEntry;

    private URI incomingLRA;

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
    private final AtomicBoolean completed = new AtomicBoolean();
    private String lifecycleFailure;

    public LraInvocationState(Method method, Class<?> resourceClass, LraMethodMetadata metadata,
            List<Object> contextsOnEntry, String authTokenOnEntry) {
        this.method = method;
        this.resourceClass = resourceClass;
        this.metadata = metadata;
        this.contextsOnEntry = List.copyOf(contextsOnEntry);
        this.authTokenOnEntry = authTokenOnEntry;
    }

    public Method getMethod() {
        return method;
    }

    public Class<?> getResourceClass() {
        return resourceClass;
    }

    public LraMethodMetadata getMetadata() {
        return metadata;
    }

    public URI getIncomingLRA() {
        return incomingLRA;
    }

    public void setIncomingLRA(URI incomingLRA) {
        this.incomingLRA = incomingLRA;
    }

    public LRA.Type getType() {
        return metadata.getPropagation();
    }

    public boolean isEndLRA() {
        return metadata.isEnd();
    }

    public Long getTimeout() {
        return metadata.getTimeLimitMillis();
    }

    public Set<Integer> getCancelOn() {
        return metadata.getCancelOn();
    }

    public Set<Outcome> getCancelOnFamily() {
        return metadata.getCancelOnFamily();
    }

    public boolean isEndAnnotation() {
        return metadata.isEndAnnotation();
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

    /**
     * @return whether the lifecycle was already completed for this invocation
     */
    public boolean isCompleted() {
        return completed.get();
    }

    /**
     * Claim the right to complete the lifecycle exactly once.
     * <p>
     * An asynchronous invocation can be completed by more than one party (the completion handler
     * and the transport's response phase), so the claim is atomic: only the caller that receives
     * {@code true} may complete it.
     *
     * @return whether this caller claimed the completion
     */
    public boolean tryMarkCompleted() {
        return completed.compareAndSet(false, true);
    }

    public String getLifecycleFailure() {
        return lifecycleFailure;
    }

    public void setLifecycleFailure(String lifecycleFailure) {
        this.lifecycleFailure = lifecycleFailure;
    }

    /**
     * Restore the thread-local state that was in effect before the intercepted invocation.
     * <p>
     * The interceptor can be nested inside other LRA processing, so it only undoes the changes made
     * by its own request phase (and re-establishes an incoming LRA that the processor suspended or
     * removed).
     */
    public void restoreCurrent() {
        if (currentLRA != null) {
            Current.removeActiveLRACache(currentLRA);
        }

        CurrentScope.restore(contextsOnEntry, authTokenOnEntry);
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
