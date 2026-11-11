/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.lifecycle;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.Type.MANDATORY;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.Type.NESTED;

import io.narayana.lra.Current;
import io.narayana.lra.api.Outcome;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.client.coordinator.LRAClient;
import io.narayana.lra.connectors.common.client.coordinator.LRAClientException;
import io.narayana.lra.connectors.common.client.discovery.MicroProfileParticipantFactory;
import io.narayana.lra.connectors.common.client.discovery.ParticipantDiscovery;
import io.narayana.lra.connectors.common.client.invocation.LRAParticipantData;
import io.narayana.lra.connectors.common.client.invocation.LraCompletion;
import io.narayana.lra.connectors.common.client.invocation.LraInvocationState;
import io.narayana.lra.connectors.common.client.invocation.LraInvocationState.Progress;
import io.narayana.lra.connectors.common.client.invocation.LraInvocationState.ProgressStep;
import io.narayana.lra.connectors.common.client.invocation.LraMethodMetadata;
import io.narayana.lra.connectors.common.client.participant.LRAParticipantRegistry;
import io.narayana.lra.connectors.common.client.participant.Participant;
import io.narayana.lra.connectors.common.client.participant.ParticipantCallbackAddressResolver;
import io.narayana.lra.connectors.common.client.participant.ParticipantIdProvider;
import io.narayana.lra.connectors.common.client.participant.protocol.ParticipantProtocolRegistry;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.inject.Inject;
import java.lang.reflect.Method;
import java.net.URI;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;

/**
 * The one lifecycle state machine.
 * <p>
 * It is transport-neutral: it only operates on a {@link LraInvocationState}, never on a request or a
 * message. A transport adapter fills the context and, when it owns completion, calls
 * {@link #processResponse} once its response exists; otherwise the interceptor does.
 */
@ApplicationScoped
public class ServerLRAProcessor implements LraLifecycleProcessor {

    private static final long DEFAULT_TIMEOUT_MILLIS = 0L;
    private static final Pattern START_END_QUOTES_PATTERN = Pattern.compile("^\"|\"$");

    // the abort status carries the value a transport would report; HTTP maps it directly
    private static final int INTERNAL_SERVER_ERROR = 500;
    private static final int PRECONDITION_FAILED = 412;

    private final LRAParticipantRegistry lraParticipantRegistry;
    private final LRAParticipantData data;
    private final int enlistMaxRetries;
    private final LRAClient lraClient;
    private final ParticipantCallbackAddressResolver callbackResolver;
    private final MicroProfileParticipantFactory participantFactory;

    @Inject
    public ServerLRAProcessor(LRAParticipantRegistry lraParticipantRegistry, LRAParticipantData data,
            @ConfigProperty(name = "lra.participant.client.max.retry", defaultValue = "3") int enlistMaxRetries,
            ParticipantProtocolRegistry protocolRegistry) {
        this.lraParticipantRegistry = lraParticipantRegistry;
        this.data = data;
        this.enlistMaxRetries = enlistMaxRetries;
        this.lraClient = protocolRegistry.client();
        this.callbackResolver = protocolRegistry.resolver();
        this.participantFactory = ParticipantDiscovery.serviceLoadedFactory();
    }

    public ServerLRAProcessor(LRAParticipantRegistry lraParticipantRegistry, LRAParticipantData data,
            int enlistMaxRetries, LRAClient lraClient,
            ParticipantCallbackAddressResolver callbackResolver) {
        this(lraParticipantRegistry, data, enlistMaxRetries, lraClient, callbackResolver,
                ParticipantDiscovery.serviceLoadedFactory());
    }

    public ServerLRAProcessor(LRAParticipantRegistry lraParticipantRegistry, LRAParticipantData data,
            int enlistMaxRetries, LRAClient lraClient,
            ParticipantCallbackAddressResolver callbackResolver, MicroProfileParticipantFactory participantFactory) {
        this.lraParticipantRegistry = lraParticipantRegistry;
        this.data = data;
        this.enlistMaxRetries = enlistMaxRetries;
        this.lraClient = lraClient;
        this.callbackResolver = Objects.requireNonNull(callbackResolver,
                "An LRA participant needs a callback address resolver");
        this.participantFactory = participantFactory;
    }

    /**
     * Process the request filter phase. This method is only a dispatcher: the behaviour of each LRA
     * annotation is implemented in its own handler method.
     *
     * @param invocation the state associated with this request
     */
    @Override
    public void processRequest(LraInvocationState invocation) {
        LraMethodMetadata metadata = invocation.getMetadata();

        // leaving is applied first, whenever there is an incoming context (with or without a transaction)
        if (metadata.isLeave() && invocation.getIncomingLRA() != null) {
            if (handleLeave(invocation)) {
                return;
            }
        }

        if (metadata.getPropagation() != null) {
            handleTransaction(invocation); // the method starts or joins an LRA
        } else {
            // participant callbacks and @Leave run with the incoming context but do not enlist;
            // methods with no LRA annotation only propagate any incoming context
            handleNoType(invocation);
        }
    }

    // the method starts or joins an LRA
    private void handleTransaction(LraInvocationState invocation) {
        if (invocation.isEndAnnotation() && invocation.getIncomingLRA() == null) {
            return;
        }

        if (invocation.getIncomingLRA() != null) {
            handleParentContext(invocation);
        }

        handleLraType(invocation);

        if (invocation.isAborted()) {
            return;
        }

        if (invocation.getCurrentLRA() == null) {
            handleNoCurrentLRA(invocation);
            return;
        }

        // store state with the current thread
        Current.push(invocation.getCurrentLRA(), null); // make the current LRA available to the called method

        if (!invocation.isEndAnnotation()) { // don't enlist for methods marked with Compensate, Complete or Leave
            handleEnlistment(invocation);
        }

        Current.addActiveLRACache(invocation.getCurrentLRA());
    }

    /**
     * Process the response filter phase: close or cancel the LRA as required and report any failures.
     *
     * @param invocation the state associated with this request
     * @param isCancel whether the response status matched the cancel conditions
     */
    @Override
    public LifecycleDecision processResponse(LraInvocationState invocation, boolean isCancel) {
        invocation.setUserData(getUserDefinedData());
        invocation.setParticipantId(ParticipantIdProvider.getParticipantId(invocation.getResourceClass()));

        // only failures recorded before the request filter aborted (plus any found below) are reported
        if (!invocation.isAborted()) {
            invocation.setProgress(null);
        }

        URI current = invocation.getCurrentLRA();

        if (current != null && isCancel) {
            invocation.setCurrentLRACancelled(true);
            handleCancellation(invocation, current);
        } else if (current != null && invocation.isEndLRA()) {
            invocation.setEndLRAClosed(true);
            handleClose(invocation);
        }

        return new LifecycleDecision(invocation.isAborted(), invocation.getAbortStatusCode(),
                invocation.getAbortMessage(), processFailures(invocation));
    }

    @Override
    public boolean isCancel(LraInvocationState invocation, Outcome outcome, boolean failed) {
        // cancel conditions only exist on @LRA; a method without it (for example @Leave only) does
        // not own the incoming LRA and must never cancel it
        if (invocation.getType() == null) {
            return false;
        }

        if (invocation.getCancelOn() != null && !invocation.getCancelOn().isEmpty()) {
            throw new IllegalStateException("cancelOn is not supported on a plain CDI bean method because there "
                    + "is no transport status; use cancelOnFamily and return io.narayana.lra.api.Result instead");
        }

        Set<Outcome> families = invocation.getCancelOnFamily();

        return families != null && families.contains(outcome);
    }

    @Override
    public boolean isCancel(LraInvocationState invocation, int statusCode) {
        if (invocation.getType() == null) {
            return false;
        }

        Set<Outcome> families = invocation.getCancelOnFamily();

        if (families != null && families.contains(LraCompletion.outcomeOf(statusCode))) {
            return true;
        }

        Set<Integer> codes = invocation.getCancelOn();

        return codes != null && codes.contains(statusCode);
    }

    /**
     * Report any failed steps (ie if progress contains any failures) to the caller.
     *
     * @param invocation the state associated with this request
     * @return a warning message describing the operations performed, or {@code null} if there is nothing to report
     */
    private String processFailures(LraInvocationState invocation) {
        List<Progress> progress = invocation.getProgress();

        if (progress == null) {
            return null;
        }

        StringJoiner badOps = new StringJoiner(", ");
        StringBuilder code = new StringBuilder("-");

        progress.forEach(p -> {
            code.append(p.progress.ordinal());
            badOps.add(String.format("%s (%s)", p.progress.name(), p.reason));
        });

        /*
         * Previously we returned a string which enabled the reader to distinguish between successful and
         * unsuccessful state transitions but a more fruitful approach is to report problems via i18n message ids
         * and leave successful ops and implicit
         */
        if (badOps.length() != 0) {
            return LRALogger.i18nLogger.warn_LRAStatusInDoubt(String.format("%s: %s", code, badOps));
        }

        return null;
    }

    // @Leave
    private boolean handleLeave(LraInvocationState invocation) {
        var body = ParticipantIdProvider.getParticipantId(invocation.getResourceClass());

        try {
            getLRAClient().leaveLRA(invocation.getIncomingLRA(), body);
            updateProgress(invocation, ProgressStep.Left, null); // leave succeeded
        } catch (LRAClientException e) {
            invocation.addProgress(ProgressStep.LeaveFailed, e.getMessage()); // leave may have failed
            abort(invocation, invocation.getIncomingLRA().toASCIIString(), e.getStatusCode(), e.getMessage());
            return true; // the error will be handled or reported via the response filter
        }

        // let the participant know which lra he left by leaving the header intact
        return false;
    }

    private void handleParentContext(LraInvocationState invocation) {
        // use the full parent LRA protocol URI
        URI firstParent = Current.getFirstParent(invocation.getIncomingLRA());

        invocation.setParentDecided(true);
        invocation.setParentLRA(firstParent);
    }

    private void handleNoType(LraInvocationState invocation) {
        if (!invocation.isEndAnnotation()) {
            Current.popAll();
            invocation.setClearIncomingLRA(true);
        }

        if (invocation.getIncomingLRA() != null) {
            invocation.setSuspendedLRA(true);
            invocation.setCurrentLRA(invocation.getIncomingLRA());
            Current.push(invocation.getIncomingLRA(), null);
            Current.addActiveLRACache(invocation.getIncomingLRA());
        }
    }

    private void handleLraType(LraInvocationState invocation) {
        switch (invocation.getType()) {
            case MANDATORY: // a txn must be present
                if (isTxInvalid(invocation, MANDATORY, invocation.getIncomingLRA(), true)) {
                    // isTxInvalid will have marked the context as aborted
                    return;
                }

                invocation.setCurrentLRA(invocation.getIncomingLRA());
                invocation.setRequiresActiveLRA(true);

                break;
            case NEVER: // a txn must not be present
                if (isTxInvalid(invocation, LRA.Type.NEVER, invocation.getIncomingLRA(), false)) {
                    return;
                }

                invocation.setCurrentLRA(null); // must not run with any context

                break;
            case NOT_SUPPORTED:
                invocation.setSuspendedLRA(invocation.getIncomingLRA() != null);
                invocation.setCurrentLRA(null); // must not run with any context

                break;
            case NESTED:
                // FALLTHROUGH
            case REQUIRED:
                if (invocation.getIncomingLRA() != null) {
                    if (invocation.getType() == NESTED) {
                        invocation.setParentDecided(true);
                        invocation.setParentLRA(invocation.getIncomingLRA());

                        // if there is an LRA present nest a new LRA under it
                        URI newLRA = startLRA(invocation, invocation.getIncomingLRA());

                        if (newLRA == null) {
                            return;
                        }

                        invocation.setCurrentLRA(newLRA);
                        invocation.setSuspendedLRA(true);
                    } else {
                        invocation.setCurrentLRA(invocation.getIncomingLRA());
                        // incomingLRA will be resumed
                        invocation.setRequiresActiveLRA(true);
                    }
                } else {
                    URI newLRA = startLRA(invocation, null);

                    if (newLRA == null) {
                        return;
                    }

                    invocation.setCurrentLRA(newLRA);
                }

                break;
            case REQUIRES_NEW:
                URI newLRA = startLRA(invocation, null);

                if (newLRA == null) {
                    return;
                }

                invocation.setCurrentLRA(newLRA);
                invocation.setSuspendedLRA(true);

                break;
            case SUPPORTS:
                invocation.setCurrentLRA(invocation.getIncomingLRA());

                // incomingLRA will be resumed if not null

                break;
            default:
                invocation.setCurrentLRA(invocation.getIncomingLRA());
        }
    }

    private void handleNoCurrentLRA(LraInvocationState invocation) {
        // the method call needs to run without a transaction
        Current.popAll();
        invocation.setClearIncomingLRA(true);
    }

    private Participant participant(Class<?> resourceClass) {
        Participant participant = lraParticipantRegistry == null
                ? null
                : lraParticipantRegistry.getParticipant(resourceClass.getName());

        if (participant != null) {
            return participant;
        }

        /*
         * No CDI container or discovery never ran (a plain JAX-RS deployment or a unit test), so
         * build the participant model from the resource class through the connector's factory.
         */
        return ParticipantDiscovery.select(resourceClass, participantFactory);
    }

    private void handleEnlistment(LraInvocationState invocation) {
        Participant participant = participant(invocation.getResourceClass());

        if (participant == null) {
            return;
        }

        ParticipantCallbacks callbacks = participant.terminationCallbacks(callbackResolver);

        long timeLimit = invocation.getTimeout() != null && callbacks.hasCompensateOrComplete()
                ? invocation.getTimeout()
                : DEFAULT_TIMEOUT_MILLIS;

        if (callbacks.hasAnyCallback()) {
            invocation.setCallbacks(callbacks);

            try {
                StringBuilder previousParticipantData = new StringBuilder();

                URI recoveryUrl = enlist(invocation, callbacks, timeLimit, previousParticipantData);

                if (previousParticipantData.length() != 0) {
                    // this participant has previously updated the LRAParticipantData bean so make it available
                    setUserDefinedData(previousParticipantData.toString());
                }

                updateProgress(invocation, ProgressStep.Joined, null);
                invocation.setRecoveryUrl(START_END_QUOTES_PATTERN.matcher(recoveryUrl.toASCIIString()).replaceAll(""));
            } catch (LRAClientException e) {
                String reason = e.getMessage();

                updateProgress(invocation, ProgressStep.JoinFailed, reason);
                abort(invocation, invocation.getCurrentLRA().toASCIIString(), e.getStatusCode(),
                        String.format("%s: %s", e.getClass().getSimpleName(), reason));
                // the failure plus any previous actions will be reported via the response filter
            }
        } else if (invocation.isRequiresActiveLRA()
                && getLRAClient().getStatus(invocation.getCurrentLRA()) != LRAStatus.Active) {
            Current.popAll();
            invocation.setClearIncomingLRA(true);
            Current.pop(invocation.getCurrentLRA());
            invocation.setSuspendedLRA(false);

            if (invocation.getType() == MANDATORY) {
                abort(invocation, invocation.getCurrentLRA().toASCIIString(), PRECONDITION_FAILED,
                        "LRA should have been active: ");
                // any previous actions will be reported via the response filter
            }
        }
    }

    private void handleCancellation(LraInvocationState invocation, URI current) {
        try {
            // do not attempt to cancel if the request filter tried but failed to start a new LRA
            if (progressDoesNotContain(invocation, ProgressStep.StartFailed)) {
                getLRAClient().cancelLRA(current, null, null);
                updateProgress(invocation, ProgressStep.Ended, null);
            }
        } catch (LRAClientException e) {
            if (e.getKind() == LRAClientException.Kind.NOT_FOUND) {
                // must already be cancelled (if the intercepted method caused it to cancel)
                // or completed (if the intercepted method caused it to complete)
                updateProgress(invocation, ProgressStep.Ended, null);
            } else {
                Method method = invocation.getMethod();
                LRALogger.i18nLogger.warn_lraFilterContainerRequest("LRAClientException: " + e.getMessage(),
                        method.getDeclaringClass().getName() + "#" + method.getName(), current.toASCIIString());

                updateProgress(invocation, ProgressStep.CancelFailed, e.getMessage());
            }
        }
    }

    private void handleClose(LraInvocationState invocation) {
        URI toClose = invocation.getCurrentLRA();

        try {
            // do not attempt to close if the request filter tried but failed to start a new LRA
            if (progressDoesNotContain(invocation, ProgressStep.StartFailed)) {
                getLRAClient().closeLRA(toClose, invocation.getParticipantId(), invocation.getUserData());

                updateProgress(invocation, ProgressStep.Ended, null);
            }
        } catch (LRAClientException e) {
            if (e.getKind() == LRAClientException.Kind.NOT_FOUND) {
                // must already be cancelled (if the intercepted method caused it to cancel)
                // or completed (if the intercepted method caused it to complete)
                updateProgress(invocation, ProgressStep.Ended, null);
            } else {
                updateProgress(invocation, ProgressStep.CloseFailed, e.getMessage());
            }
        }
    }

    private boolean isTxInvalid(LraInvocationState invocation, LRA.Type type, URI lraId, boolean shouldNotBeNull) {
        if (lraId == null && shouldNotBeNull) {
            abort(invocation, null, PRECONDITION_FAILED, type.name() + " but no tx");
            return true;
        } else if (lraId != null && !shouldNotBeNull) {
            abort(invocation, lraId.toASCIIString(), PRECONDITION_FAILED, type.name() + " but found tx");
            return true;
        }

        return false;
    }

    private URI startLRA(LraInvocationState invocation, URI parentLRA) {
        // timeout should already have been converted to milliseconds
        Method method = invocation.getMethod();
        String clientId = method.getDeclaringClass().getName() + "#" + method.getName();

        try {
            URI lra = getLRAClient().startLRA(parentLRA, clientId, invocation.getTimeout(), ChronoUnit.MILLIS, false);
            updateProgress(invocation, ProgressStep.Started, null);
            return lra;
        } catch (LRAClientException e) {
            String msg = e.getMessage();

            updateProgress(invocation, ProgressStep.StartFailed, msg);

            abort(invocation, null, e.getStatusCode(), msg);
        }

        return null;
    }

    private URI enlist(LraInvocationState invocation, ParticipantCallbacks callbacks, long timeLimit,
            StringBuilder previousParticipantData) {
        // The coordinator needs to hold a lock to enlist an participant. This lock is only waited on for a small amount of
        // time (potentially even zero time). This means if multiple participants try to enlist at the same time, enlistment
        // may fail. We therefore re-try a configurable amount of times.
        for (int i = 0;; i++) {
            try {
                return getLRAClient().enlistCompensator(invocation.getCurrentLRA(), timeLimit, callbacks,
                        previousParticipantData,
                        ParticipantIdProvider.getParticipantId(invocation.getResourceClass()));
            } catch (LRAClientException e) {
                String logMessage = "The coordinator service is unavailable; failing in enlisting the compensator for "
                        + invocation.getCurrentLRA()
                        + ", ENLIST_PARTICIPANT_CLIENT_MAX_RETRY (via MP config property \"lra.participant.client.max.retry\") set to "
                        + enlistMaxRetries + ". ";

                if (e.getKind() != LRAClientException.Kind.UNAVAILABLE || i >= enlistMaxRetries) {
                    LRALogger.logger.warn(logMessage + "Not retrying.");
                    throw e;
                }

                LRALogger.logger.warn(logMessage + " Attempt " + (i + 1) + " of " + enlistMaxRetries + ".");
            }
        }
    }

    private boolean progressDoesNotContain(LraInvocationState invocation, ProgressStep step) {
        List<Progress> progress = invocation.getProgress();
        return progress == null || progress.stream().noneMatch(p -> p.progress == step);
    }

    // add another step to the list of steps performed so far
    private void updateProgress(LraInvocationState invocation, ProgressStep step, String reason) {
        if (reason == null) {
            LRALogger.logger.debug(step.toString());
        } else {
            invocation.addProgress(step, reason);
        }
    }

    // the processing performed by the request phase caused the request to abort (without executing application code)
    private void abort(LraInvocationState invocation, String lraId, int statusCode, String message) {
        invocation.setAborted(true);
        invocation.setAbortStatusCode(statusCode);
        invocation.setAbortMessage(message);

        Method method = invocation.getMethod();
        LRALogger.i18nLogger.warn_lraFilterContainerRequest(message,
                method.getDeclaringClass().getName() + "#" + method.getName(),
                lraId == null ? "context" : lraId);
    }

    private LRAClient getLRAClient() {
        return lraClient;
    }

    private String getUserDefinedData() {
        try {
            return data != null ? data.getData() : null;
        } catch (ContextNotActiveException e) {
            LRALogger.i18nLogger.warn_missingContexts("LRAParticipantData is not usable in this (probably async) invocation.");
        }

        return null;
    }

    private void setUserDefinedData(String userDefinedData) {
        try {
            if (data != null) {
                data.setData(userDefinedData);
            }
        } catch (ContextNotActiveException e) {
            LRALogger.i18nLogger.warn_missingContexts("LRAParticipantData is not usable in this (probably async) invocation.");
        }
    }
}
