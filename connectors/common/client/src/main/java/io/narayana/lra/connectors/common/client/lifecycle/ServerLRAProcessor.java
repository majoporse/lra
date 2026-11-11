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
import io.narayana.lra.connectors.common.client.invocation.LraInvocation;
import io.narayana.lra.connectors.common.client.invocation.LraInvocation.Progress;
import io.narayana.lra.connectors.common.client.invocation.LraInvocation.ProgressStep;
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
 * It is transport-neutral: it only operates on a {@link LraInvocation}, never on a request or a
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
     * @param context the state associated with this request
     */
    @Override
    public void processRequest(LraInvocation context) {
        LraMethodMetadata metadata = context.getMetadata();

        // leaving is applied first, whenever there is an incoming context (with or without a transaction)
        if (metadata.isLeave() && context.getIncomingLRA() != null) {
            if (handleLeave(context)) {
                return;
            }
        }

        if (metadata.getPropagation() != null) {
            handleTransaction(context); // the method starts or joins an LRA
        } else {
            // participant callbacks and @Leave run with the incoming context but do not enlist;
            // methods with no LRA annotation only propagate any incoming context
            handleNoType(context);
        }
    }

    // the method starts or joins an LRA
    private void handleTransaction(LraInvocation context) {
        if (context.isEndAnnotation() && context.getIncomingLRA() == null) {
            return;
        }

        if (context.getIncomingLRA() != null) {
            handleParentContext(context);
        }

        handleLraType(context);

        if (context.isAborted()) {
            return;
        }

        if (context.getCurrentLRA() == null) {
            handleNoCurrentLRA(context);
            return;
        }

        // store state with the current thread
        Current.push(context.getCurrentLRA(), null); // make the current LRA available to the called method

        if (!context.isEndAnnotation()) { // don't enlist for methods marked with Compensate, Complete or Leave
            handleEnlistment(context);
        }

        Current.addActiveLRACache(context.getCurrentLRA());
    }

    /**
     * Process the response filter phase: close or cancel the LRA as required and report any failures.
     *
     * @param context the state associated with this request
     * @param isCancel whether the response status matched the cancel conditions
     */
    @Override
    public LifecycleDecision processResponse(LraInvocation context, boolean isCancel) {
        context.setUserData(getUserDefinedData());
        context.setParticipantId(ParticipantIdProvider.getParticipantId(context.getResourceClass()));

        // only failures recorded before the request filter aborted (plus any found below) are reported
        if (!context.isAborted()) {
            context.setProgress(null);
        }

        URI current = context.getCurrentLRA();

        if (current != null && isCancel) {
            context.setCurrentLRACancelled(true);
            handleCancellation(context, current);
        } else if (current != null && context.isEndLRA()) {
            context.setEndLRAClosed(true);
            handleClose(context);
        }

        return new LifecycleDecision(context.isAborted(), context.getAbortStatusCode(),
                context.getAbortMessage(), processFailures(context));
    }

    @Override
    public boolean isCancel(LraInvocation context, Outcome outcome, boolean failed) {
        // cancel conditions only exist on @LRA; a method without it (for example @Leave only) does
        // not own the incoming LRA and must never cancel it
        if (context.getType() == null) {
            return false;
        }

        if (context.getCancelOn() != null && !context.getCancelOn().isEmpty()) {
            throw new IllegalStateException("cancelOn is not supported on a plain CDI bean method because there "
                    + "is no transport status; use cancelOnFamily and return io.narayana.lra.api.Result instead");
        }

        Set<Outcome> families = context.getCancelOnFamily();

        return families != null && families.contains(outcome);
    }

    @Override
    public boolean isCancel(LraInvocation context, int statusCode) {
        if (context.getType() == null) {
            return false;
        }

        Set<Outcome> families = context.getCancelOnFamily();

        if (families != null && families.contains(LraCompletion.outcomeOf(statusCode))) {
            return true;
        }

        Set<Integer> codes = context.getCancelOn();

        return codes != null && codes.contains(statusCode);
    }

    /**
     * Report any failed steps (ie if progress contains any failures) to the caller.
     *
     * @param context the state associated with this request
     * @return a warning message describing the operations performed, or {@code null} if there is nothing to report
     */
    private String processFailures(LraInvocation context) {
        List<Progress> progress = context.getProgress();

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
    private boolean handleLeave(LraInvocation context) {
        var body = ParticipantIdProvider.getParticipantId(context.getResourceClass());

        try {
            getLRAClient().leaveLRA(context.getIncomingLRA(), body);
            updateProgress(context, ProgressStep.Left, null); // leave succeeded
        } catch (LRAClientException e) {
            context.addProgress(ProgressStep.LeaveFailed, e.getMessage()); // leave may have failed
            abort(context, context.getIncomingLRA().toASCIIString(), e.getStatusCode(), e.getMessage());
            return true; // the error will be handled or reported via the response filter
        }

        // let the participant know which lra he left by leaving the header intact
        return false;
    }

    private void handleParentContext(LraInvocation context) {
        // use the full parent LRA protocol URI
        URI firstParent = Current.getFirstParent(context.getIncomingLRA());

        context.setParentDecided(true);
        context.setParentLRA(firstParent);
    }

    private void handleNoType(LraInvocation context) {
        if (!context.isEndAnnotation()) {
            Current.popAll();
            context.setClearIncomingLRA(true);
        }

        if (context.getIncomingLRA() != null) {
            context.setSuspendedLRA(true);
            context.setCurrentLRA(context.getIncomingLRA());
            Current.push(context.getIncomingLRA(), null);
            Current.addActiveLRACache(context.getIncomingLRA());
        }
    }

    private void handleLraType(LraInvocation context) {
        switch (context.getType()) {
            case MANDATORY: // a txn must be present
                if (isTxInvalid(context, MANDATORY, context.getIncomingLRA(), true)) {
                    // isTxInvalid will have marked the context as aborted
                    return;
                }

                context.setCurrentLRA(context.getIncomingLRA());
                context.setRequiresActiveLRA(true);

                break;
            case NEVER: // a txn must not be present
                if (isTxInvalid(context, LRA.Type.NEVER, context.getIncomingLRA(), false)) {
                    return;
                }

                context.setCurrentLRA(null); // must not run with any context

                break;
            case NOT_SUPPORTED:
                context.setSuspendedLRA(context.getIncomingLRA() != null);
                context.setCurrentLRA(null); // must not run with any context

                break;
            case NESTED:
                // FALLTHROUGH
            case REQUIRED:
                if (context.getIncomingLRA() != null) {
                    if (context.getType() == NESTED) {
                        context.setParentDecided(true);
                        context.setParentLRA(context.getIncomingLRA());

                        // if there is an LRA present nest a new LRA under it
                        URI newLRA = startLRA(context, context.getIncomingLRA());

                        if (newLRA == null) {
                            return;
                        }

                        context.setCurrentLRA(newLRA);
                        context.setSuspendedLRA(true);
                    } else {
                        context.setCurrentLRA(context.getIncomingLRA());
                        // incomingLRA will be resumed
                        context.setRequiresActiveLRA(true);
                    }
                } else {
                    URI newLRA = startLRA(context, null);

                    if (newLRA == null) {
                        return;
                    }

                    context.setCurrentLRA(newLRA);
                }

                break;
            case REQUIRES_NEW:
                URI newLRA = startLRA(context, null);

                if (newLRA == null) {
                    return;
                }

                context.setCurrentLRA(newLRA);
                context.setSuspendedLRA(true);

                break;
            case SUPPORTS:
                context.setCurrentLRA(context.getIncomingLRA());

                // incomingLRA will be resumed if not null

                break;
            default:
                context.setCurrentLRA(context.getIncomingLRA());
        }
    }

    private void handleNoCurrentLRA(LraInvocation context) {
        // the method call needs to run without a transaction
        Current.popAll();
        context.setClearIncomingLRA(true);
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

    private void handleEnlistment(LraInvocation context) {
        Participant participant = participant(context.getResourceClass());

        if (participant == null) {
            return;
        }

        ParticipantCallbacks callbacks = participant.terminationCallbacks(callbackResolver);

        long timeLimit = context.getTimeout() != null && callbacks.hasCompensateOrComplete()
                ? context.getTimeout()
                : DEFAULT_TIMEOUT_MILLIS;

        if (callbacks.hasAnyCallback()) {
            context.setCallbacks(callbacks);

            try {
                StringBuilder previousParticipantData = new StringBuilder();

                URI recoveryUrl = enlist(context, callbacks, timeLimit, previousParticipantData);

                if (previousParticipantData.length() != 0) {
                    // this participant has previously updated the LRAParticipantData bean so make it available
                    setUserDefinedData(previousParticipantData.toString());
                }

                updateProgress(context, ProgressStep.Joined, null);
                context.setRecoveryUrl(START_END_QUOTES_PATTERN.matcher(recoveryUrl.toASCIIString()).replaceAll(""));
            } catch (LRAClientException e) {
                String reason = e.getMessage();

                updateProgress(context, ProgressStep.JoinFailed, reason);
                abort(context, context.getCurrentLRA().toASCIIString(), e.getStatusCode(),
                        String.format("%s: %s", e.getClass().getSimpleName(), reason));
                // the failure plus any previous actions will be reported via the response filter
            }
        } else if (context.isRequiresActiveLRA()
                && getLRAClient().getStatus(context.getCurrentLRA()) != LRAStatus.Active) {
            Current.popAll();
            context.setClearIncomingLRA(true);
            Current.pop(context.getCurrentLRA());
            context.setSuspendedLRA(false);

            if (context.getType() == MANDATORY) {
                abort(context, context.getCurrentLRA().toASCIIString(), PRECONDITION_FAILED,
                        "LRA should have been active: ");
                // any previous actions will be reported via the response filter
            }
        }
    }

    private void handleCancellation(LraInvocation context, URI current) {
        try {
            // do not attempt to cancel if the request filter tried but failed to start a new LRA
            if (progressDoesNotContain(context, ProgressStep.StartFailed)) {
                getLRAClient().cancelLRA(current, null, null);
                updateProgress(context, ProgressStep.Ended, null);
            }
        } catch (LRAClientException e) {
            if (e.getKind() == LRAClientException.Kind.NOT_FOUND) {
                // must already be cancelled (if the intercepted method caused it to cancel)
                // or completed (if the intercepted method caused it to complete)
                updateProgress(context, ProgressStep.Ended, null);
            } else {
                Method method = context.getMethod();
                LRALogger.i18nLogger.warn_lraFilterContainerRequest("LRAClientException: " + e.getMessage(),
                        method.getDeclaringClass().getName() + "#" + method.getName(), current.toASCIIString());

                updateProgress(context, ProgressStep.CancelFailed, e.getMessage());
            }
        }
    }

    private void handleClose(LraInvocation context) {
        URI toClose = context.getCurrentLRA();

        try {
            // do not attempt to close if the request filter tried but failed to start a new LRA
            if (progressDoesNotContain(context, ProgressStep.StartFailed)) {
                getLRAClient().closeLRA(toClose, context.getParticipantId(), context.getUserData());

                updateProgress(context, ProgressStep.Ended, null);
            }
        } catch (LRAClientException e) {
            if (e.getKind() == LRAClientException.Kind.NOT_FOUND) {
                // must already be cancelled (if the intercepted method caused it to cancel)
                // or completed (if the intercepted method caused it to complete)
                updateProgress(context, ProgressStep.Ended, null);
            } else {
                updateProgress(context, ProgressStep.CloseFailed, e.getMessage());
            }
        }
    }

    private boolean isTxInvalid(LraInvocation context, LRA.Type type, URI lraId, boolean shouldNotBeNull) {
        if (lraId == null && shouldNotBeNull) {
            abort(context, null, PRECONDITION_FAILED, type.name() + " but no tx");
            return true;
        } else if (lraId != null && !shouldNotBeNull) {
            abort(context, lraId.toASCIIString(), PRECONDITION_FAILED, type.name() + " but found tx");
            return true;
        }

        return false;
    }

    private URI startLRA(LraInvocation context, URI parentLRA) {
        // timeout should already have been converted to milliseconds
        Method method = context.getMethod();
        String clientId = method.getDeclaringClass().getName() + "#" + method.getName();

        try {
            URI lra = getLRAClient().startLRA(parentLRA, clientId, context.getTimeout(), ChronoUnit.MILLIS, false);
            updateProgress(context, ProgressStep.Started, null);
            return lra;
        } catch (LRAClientException e) {
            String msg = e.getMessage();

            updateProgress(context, ProgressStep.StartFailed, msg);

            abort(context, null, e.getStatusCode(), msg);
        }

        return null;
    }

    private URI enlist(LraInvocation context, ParticipantCallbacks callbacks, long timeLimit,
            StringBuilder previousParticipantData) {
        // The coordinator needs to hold a lock to enlist an participant. This lock is only waited on for a small amount of
        // time (potentially even zero time). This means if multiple participants try to enlist at the same time, enlistment
        // may fail. We therefore re-try a configurable amount of times.
        for (int i = 0;; i++) {
            try {
                return getLRAClient().enlistCompensator(context.getCurrentLRA(), timeLimit, callbacks,
                        previousParticipantData,
                        ParticipantIdProvider.getParticipantId(context.getResourceClass()));
            } catch (LRAClientException e) {
                String logMessage = "The coordinator service is unavailable; failing in enlisting the compensator for "
                        + context.getCurrentLRA()
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

    private boolean progressDoesNotContain(LraInvocation context, ProgressStep step) {
        List<Progress> progress = context.getProgress();
        return progress == null || progress.stream().noneMatch(p -> p.progress == step);
    }

    // add another step to the list of steps performed so far
    private void updateProgress(LraInvocation context, ProgressStep step, String reason) {
        if (reason == null) {
            LRALogger.logger.debug(step.toString());
        } else {
            context.addProgress(step, reason);
        }
    }

    // the processing performed by the request phase caused the request to abort (without executing application code)
    private void abort(LraInvocation context, String lraId, int statusCode, String message) {
        context.setAborted(true);
        context.setAbortStatusCode(statusCode);
        context.setAbortMessage(message);

        Method method = context.getMethod();
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
            LRALogger.i18nLogger.warn_missingContexts("LRAParticipantData is not usable in this (probably async) context.");
        }

        return null;
    }

    private void setUserDefinedData(String userDefinedData) {
        try {
            if (data != null) {
                data.setData(userDefinedData);
            }
        } catch (ContextNotActiveException e) {
            LRALogger.i18nLogger.warn_missingContexts("LRAParticipantData is not usable in this (probably async) context.");
        }
    }
}
