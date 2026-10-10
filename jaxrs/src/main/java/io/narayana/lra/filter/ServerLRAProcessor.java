/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.filter;

import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;
import static jakarta.ws.rs.core.Response.Status.PRECONDITION_FAILED;
import static jakarta.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.Type.MANDATORY;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.Type.NESTED;

import io.narayana.lra.AnnotationResolver;
import io.narayana.lra.BearerTokenResolver;
import io.narayana.lra.Current;
import io.narayana.lra.PropagateToken;
import io.narayana.lra.callbacks.ParticipantCallbacks;
import io.narayana.lra.client.LRAParticipantData;
import io.narayana.lra.client.NarayanaLRAClient;
import io.narayana.lra.client.ParticipantIdProvider;
import io.narayana.lra.client.internal.proxy.nonjaxrs.LRAParticipant;
import io.narayana.lra.client.internal.proxy.nonjaxrs.LRAParticipantRegistry;
import io.narayana.lra.filter.ServerLRAContext.Progress;
import io.narayana.lra.filter.ServerLRAContext.ProgressStep;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.Method;
import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.Forget;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.Status;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.eclipse.microprofile.lra.annotation.ws.rs.Leave;

/**
 * Implements the LRA annotation handling for a single JAX-RS request/response.
 * <p>
 * This class is deliberately decoupled from the HTTP transport: it only operates on a
 * {@link ServerLRAContext} and never touches the JAX-RS request or response containers. Each LRA
 * annotation is handled by a dedicated method and the transport specific concerns (headers,
 * aborting the request, the response entity, ...) are dealt with by {@link ServerLRAFilter}.
 */
public class ServerLRAProcessor {

    private static final long DEFAULT_TIMEOUT_MILLIS = 0L;
    private static final Pattern START_END_QUOTES_PATTERN = Pattern.compile("^\"|\"$");

    private final LRAParticipantRegistry lraParticipantRegistry;
    private final LRAParticipantData data;
    private final int enlistMaxRetries;

    private NarayanaLRAClient lraClient;

    public ServerLRAProcessor(LRAParticipantRegistry lraParticipantRegistry, LRAParticipantData data,
            int enlistMaxRetries) {
        this.lraParticipantRegistry = lraParticipantRegistry;
        this.data = data;
        this.enlistMaxRetries = enlistMaxRetries;
    }

    /**
     * Process the request filter phase. This method is only a dispatcher: the behaviour of each LRA
     * annotation is implemented in its own handler method.
     *
     * @param context the state associated with this request
     */
    public void processRequest(ServerLRAContext context) {
        Method method = context.getMethod();

        // @PropagateToken is orthogonal to the LRA lifecycle
        handlePropagateToken(context);

        // @Leave is applied first, whenever there is an incoming context (with or without @LRA)
        if (isLeavePresent(method) && context.getIncomingLRA() != null) {
            if (handleLeave(context)) {
                return;
            }
        }

        LRA lra = resolveLRA(method);

        if (lra != null) {
            handleLRA(context, lra); // @LRA
        } else {
            // @Compensate, @Complete, @Status, @Forget, @AfterLRA and @Leave run with the incoming context
            // but do not enlist; methods with no LRA annotation only propagate any incoming context
            context.setEndAnnotation(isParticipantCallback(method) || isLeavePresent(method));
            handleNoType(context);
        }
    }

    // @LRA
    private void handleLRA(ServerLRAContext context, LRA transactional) {
        context.setType(transactional.value());
        context.setLongRunning(!transactional.end());

        Response.Status.Family[] cancelOnFamily = transactional.cancelOnFamily();
        Response.Status[] cancelOn = transactional.cancelOn();

        if (cancelOnFamily.length != 0) {
            context.setCancelOnFamily(cancelOnFamily);
        }

        if (cancelOn.length != 0) {
            context.setCancelOn(cancelOn);
        }

        if (transactional.timeLimit() != 0) {
            context.setTimeout(Duration.of(transactional.timeLimit(), transactional.timeUnit()).toMillis());
        }

        resolveEndAnnotation(context);

        // check the incoming request for an LRA context
        if (context.getIncomingLRA() == null && context.getContextPropertyLRA() != null) {
            context.setIncomingLRA(context.getContextPropertyLRA());
        }

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

        if (!context.isLongRunning()) {
            context.setTerminalLRA(context.getCurrentLRA());
        }

        // store state with the current thread
        Current.push(context.getCurrentLRA(), null); // make the current LRA available to the called method

        if (context.getNewLRA() != null && context.getSuspendedLRA() != null) {
            context.setSuspendedContextLRA(context.getIncomingLRA());
        }

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
    public void processResponse(ServerLRAContext context, boolean isCancel) {
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
        } else if (context.getTerminalLRA() != null) {
            context.setTerminalLRAClosed(true);
            handleClose(context);
        }
    }

    /**
     * Report any failed steps (ie if progress contains any failures) to the caller.
     *
     * @param context the state associated with this request
     * @return a warning message describing the operations performed, or {@code null} if there is nothing to report
     */
    public String processFailures(ServerLRAContext context) {
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

    // @PropagateToken
    private void handlePropagateToken(ServerLRAContext context) {
        Method method = context.getMethod();

        if (AnnotationResolver.isAnnotationPresent(PropagateToken.class, method)
                || method.getDeclaringClass().isAnnotationPresent(PropagateToken.class)) {
            String authHeader = context.getAuthHeader();

            if (authHeader != null && authHeader.regionMatches(true, 0, "Bearer ", 0, 7)) {
                String token = authHeader.substring(7);

                if (BearerTokenResolver.isPlausibleJwt(token)) {
                    Current.setAuthToken(token);
                } else {
                    LRALogger.logger.warnf("@PropagateToken: Authorization header does not contain a valid JWT structure");
                }
            }
        }
    }

    private LRA resolveLRA(Method method) {
        LRA transactional = AnnotationResolver.resolveAnnotation(LRA.class, method);

        if (transactional == null) {
            transactional = method.getDeclaringClass().getDeclaredAnnotation(LRA.class);
        }

        return transactional;
    }

    private boolean isParticipantCallback(Method method) {
        return AnnotationResolver.isAnnotationPresent(Compensate.class, method)
                || AnnotationResolver.isAnnotationPresent(Complete.class, method)
                || AnnotationResolver.isAnnotationPresent(Status.class, method)
                || AnnotationResolver.isAnnotationPresent(Forget.class, method)
                || AnnotationResolver.isAnnotationPresent(AfterLRA.class, method);
    }

    // @Leave
    private boolean handleLeave(ServerLRAContext context) {
        var body = ParticipantIdProvider.getParticipantId(context.getResourceClass());

        try {
            getLRAClient().leaveLRA(context.getIncomingLRA(), body);
            context.addProgress(ProgressStep.Left, null); // leave succeeded
        } catch (WebApplicationException e) {
            context.addProgress(ProgressStep.LeaveFailed, e.getMessage()); // leave may have failed
            abort(context, context.getIncomingLRA().toASCIIString(),
                    e.getResponse().getStatus(), e.getMessage());
            return true; // the error will be handled or reported via the response filter
        } catch (ProcessingException e) { // a remote coordinator was unavailable
            context.addProgress(ProgressStep.LeaveFailed, e.getMessage()); // leave may have failed
            abort(context, context.getIncomingLRA().toASCIIString(),
                    INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage());
            return true; // the error will be handled or reported via the response filter
        }

        // let the participant know which lra he left by leaving the header intact
        return false;
    }

    private void handleParentContext(ServerLRAContext context) {
        // use the full parent LRA protocol URI
        URI firstParent = Current.getFirstParent(context.getIncomingLRA());

        context.setParentHeaderHandled(true);
        context.setParentLRA(firstParent);
    }

    private void handleNoType(ServerLRAContext context) {
        if (!context.isEndAnnotation()) {
            Current.popAll();
            context.setClearContextHeader(true);
        }

        if (context.getIncomingLRA() != null) {
            context.setSuspendedLRA(context.getIncomingLRA());
            context.setSuspendedContextLRA(context.getIncomingLRA());
            context.setCurrentLRA(context.getIncomingLRA());
            Current.push(context.getIncomingLRA(), null);
            Current.addActiveLRACache(context.getIncomingLRA());
        }
    }

    private void handleLraType(ServerLRAContext context) {
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
                context.setSuspendedLRA(context.getIncomingLRA());
                context.setCurrentLRA(null); // must not run with any context

                break;
            case NESTED:
                // FALLTHROUGH
            case REQUIRED:
                if (context.getIncomingLRA() != null) {
                    if (context.getType() == NESTED) {
                        context.setParentHeaderHandled(true);
                        context.setParentLRA(context.getIncomingLRA());

                        // if there is an LRA present nest a new LRA under it
                        context.setSuspendedLRA(context.getIncomingLRA());

                        URI newLRA = startLRA(context, context.getIncomingLRA());

                        if (newLRA == null) {
                            return;
                        }

                        context.setNewLRA(newLRA);
                        context.setCurrentLRA(newLRA);
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

                    context.setNewLRA(newLRA);
                    context.setCurrentLRA(newLRA);
                }

                break;
            case REQUIRES_NEW:
                context.setSuspendedLRA(context.getIncomingLRA());

                URI newLRA = startLRA(context, null);

                if (newLRA == null) {
                    return;
                }

                context.setNewLRA(newLRA);
                context.setCurrentLRA(newLRA);

                break;
            case SUPPORTS:
                context.setCurrentLRA(context.getIncomingLRA());

                // incomingLRA will be resumed if not null

                break;
            default:
                context.setCurrentLRA(context.getIncomingLRA());
        }
    }

    private void handleNoCurrentLRA(ServerLRAContext context) {
        // the method call needs to run without a transaction
        Current.popAll();
        context.setClearContextHeader(true);

        if (context.getSuspendedLRA() != null) {
            context.setSuspendedContextLRA(context.getSuspendedLRA());
        }
    }

    private void handleEnlistment(ServerLRAContext context) {
        ParticipantCallbacks callbacks = NarayanaLRAClient.getTerminationCallbacks(
                context.getResourceClass(), context.getUriPrefix());
        long timeLimit = context.getTimeout() != null && callbacks.hasCompensateOrComplete()
                ? context.getTimeout()
                : DEFAULT_TIMEOUT_MILLIS;

        LRAParticipant participant = lraParticipantRegistry != null
                ? lraParticipantRegistry.getParticipant(context.getResourceClass().getName())
                : null;

        if (callbacks.hasAnyCallback() || participant != null) {
            context.setCallbacks(callbacks);

            try {
                if (participant != null) {
                    participant.augmentTerminationCallbacks(callbacks, context.getBaseUri());
                }

                StringBuilder previousParticipantData = new StringBuilder();

                URI recoveryUrl = enlist(context, callbacks, timeLimit, previousParticipantData);

                if (previousParticipantData.length() != 0) {
                    // this participant has previously updated the LRAParticipantData bean so make it available
                    setUserDefinedData(previousParticipantData.toString());
                }

                updateProgress(context, ProgressStep.Joined, null);
                context.setRecoveryUrl(START_END_QUOTES_PATTERN.matcher(recoveryUrl.toASCIIString()).replaceAll(""));
            } catch (WebApplicationException e) {
                String reason = e.getResponse().readEntity(String.class);

                updateProgress(context, ProgressStep.JoinFailed, reason);
                abort(context, context.getCurrentLRA().toASCIIString(),
                        e.getResponse().getStatus(),
                        String.format("%s: %s", e.getClass().getSimpleName(), reason));
                // the failure plus any previous actions will be reported via the response filter
            } catch (ProcessingException e) {
                updateProgress(context, ProgressStep.JoinFailed, e.getMessage()); // a remote coordinator was unavailable
                abort(context, context.getCurrentLRA().toASCIIString(),
                        INTERNAL_SERVER_ERROR.getStatusCode(),
                        String.format("%s %s,", e.getClass().getSimpleName(), e.getMessage()));
                // the failure plus any previous actions will be reported via the response filter
            }
        } else if (context.isRequiresActiveLRA() && getLRAClient().getStatus(context.getCurrentLRA()) != LRAStatus.Active) {
            Current.popAll();
            context.setClearContextHeader(true);
            Current.pop(context.getCurrentLRA());
            context.setSuspendedLRA(null);
            context.setSuspendedContextLRA(null);

            if (context.getType() == MANDATORY) {
                abort(context, context.getCurrentLRA().toASCIIString(),
                        PRECONDITION_FAILED.getStatusCode(),
                        "LRA should have been active: ");
                // any previous actions will be reported via the response filter
            }
        }
    }

    private void handleCancellation(ServerLRAContext context, URI current) {
        try {
            // do not attempt to cancel if the request filter tried but failed to start a new LRA
            if (progressDoesNotContain(context, ProgressStep.StartFailed)) {
                getLRAClient().cancelLRA(current);
                updateProgress(context, ProgressStep.Ended, null);
            }
        } catch (NotFoundException ignore) {
            // must already be cancelled (if the intercepted method caused it to cancel)
            // or completed (if the intercepted method caused it to complete)
            updateProgress(context, ProgressStep.Ended, null);
        } catch (WebApplicationException e) {
            updateProgress(context, ProgressStep.CancelFailed, e.getMessage());
        } catch (ProcessingException e) {
            Method method = context.getMethod();
            LRALogger.i18nLogger.warn_lraFilterContainerRequest("ProcessingException: " + e.getMessage(),
                    method.getDeclaringClass().getName() + "#" + method.getName(), current.toASCIIString());

            updateProgress(context, ProgressStep.CancelFailed, e.getMessage());
            context.setTerminalLRA(null);
        }
    }

    private void handleClose(ServerLRAContext context) {
        URI toClose = context.getTerminalLRA();

        try {
            // do not attempt to close if the request filter tried but failed to start a new LRA
            if (progressDoesNotContain(context, ProgressStep.StartFailed)) {
                getLRAClient().closeLRA(toClose, context.getParticipantId(), context.getUserData());

                updateProgress(context, ProgressStep.Ended, null);
            }
        } catch (WebApplicationException e) {
            if (e.getResponse().getStatus() == NOT_FOUND.getStatusCode()) {
                // must already be cancelled (if the intercepted method caused it to cancel)
                // or completed (if the intercepted method caused it to complete)
                updateProgress(context, ProgressStep.Ended, null);
            } else {
                // same as ProcessingException case
                updateProgress(context, ProgressStep.CloseFailed, e.getMessage());
            }
        } catch (ProcessingException e) {
            updateProgress(context, ProgressStep.CloseFailed, e.getMessage());
        }
    }

    private boolean isTxInvalid(ServerLRAContext context, LRA.Type type, URI lraId, boolean shouldNotBeNull) {
        if (lraId == null && shouldNotBeNull) {
            abort(context, null, PRECONDITION_FAILED.getStatusCode(), type.name() + " but no tx");
            return true;
        } else if (lraId != null && !shouldNotBeNull) {
            abort(context, lraId.toASCIIString(), PRECONDITION_FAILED.getStatusCode(), type.name() + " but found tx");
            return true;
        }

        return false;
    }

    private URI startLRA(ServerLRAContext context, URI parentLRA) {
        // timeout should already have been converted to milliseconds
        Method method = context.getMethod();
        String clientId = method.getDeclaringClass().getName() + "#" + method.getName();

        try {
            URI lra = getLRAClient().startLRA(parentLRA, clientId, context.getTimeout(), ChronoUnit.MILLIS, false);
            updateProgress(context, ProgressStep.Started, null);
            return lra;
        } catch (WebApplicationException e) {
            String msg = e.getResponse().readEntity(String.class);

            updateProgress(context, ProgressStep.StartFailed, msg);

            abort(context, null, e.getResponse().getStatus(), msg);
        }

        return null;
    }

    private URI enlist(ServerLRAContext context, ParticipantCallbacks callbacks, long timeLimit,
            StringBuilder previousParticipantData) {
        // The coordinator needs to hold a lock to enlist an participant. This lock is only waited on for a small amount of
        // time (potentially even zero time). This means if multiple participants try to enlist at the same time, enlistment
        // may fail. We therefore re-try a configurable amount of times.
        for (int i = 0;; i++) {
            try {
                return getLRAClient().enlistCompensator(context.getCurrentLRA(), timeLimit, callbacks,
                        previousParticipantData,
                        ParticipantIdProvider.getParticipantId(context.getResourceClass()));
            } catch (WebApplicationException e) {
                String logMessage = "The coordinator service is unavailable; failing in enlisting the compensator for "
                        + context.getCurrentLRA()
                        + ", ENLIST_PARTICIPANT_CLIENT_MAX_RETRY (via MP config property \"lra.participant.client.max.retry\") set to "
                        + enlistMaxRetries + ". ";

                if (e.getResponse().getStatus() != SERVICE_UNAVAILABLE.getStatusCode() || i >= enlistMaxRetries) {
                    LRALogger.logger.warn(logMessage + "Not retrying.");
                    throw e;
                }

                LRALogger.logger.warn(logMessage + " Attempt " + (i + 1) + " of " + enlistMaxRetries + ".");
            }
        }
    }

    // a method is an "end annotation" method if it is a participant callback or only leaves an LRA.
    // such methods run with the incoming context but are not themselves enlisted.
    private void resolveEndAnnotation(ServerLRAContext context) {
        Method method = context.getMethod();

        context.setEndAnnotation(
                AnnotationResolver.isAnnotationPresent(Complete.class, method)
                        || AnnotationResolver.isAnnotationPresent(Compensate.class, method)
                        || AnnotationResolver.isAnnotationPresent(Leave.class, method)
                        || AnnotationResolver.isAnnotationPresent(Status.class, method)
                        || AnnotationResolver.isAnnotationPresent(Forget.class, method)
                        || AnnotationResolver.isAnnotationPresent(AfterLRA.class, method));
    }

    private boolean isLeavePresent(Method method) {
        return AnnotationResolver.isAnnotationPresent(Leave.class, method);
    }

    private boolean progressDoesNotContain(ServerLRAContext context, ProgressStep step) {
        List<Progress> progress = context.getProgress();
        return progress == null || progress.stream().noneMatch(p -> p.progress == step);
    }

    // add another step to the list of steps performed so far
    private void updateProgress(ServerLRAContext context, ProgressStep step, String reason) {
        if (reason == null) {
            LRALogger.logger.debug(step.toString());
        } else {
            context.addProgress(step, reason);
        }
    }

    // the processing performed by the request filter caused the request to abort (without executing application code)
    private void abort(ServerLRAContext context, String lraId, int statusCode, String message) {
        context.setAborted(true);
        context.setAbortStatusCode(statusCode);
        context.setAbortMessage(message);

        Method method = context.getMethod();
        LRALogger.i18nLogger.warn_lraFilterContainerRequest(message,
                method.getDeclaringClass().getName() + "#" + method.getName(),
                lraId == null ? "context" : lraId);
    }

    private NarayanaLRAClient getLRAClient() {
        if (lraClient == null) {
            // no need to lock
            lraClient = new NarayanaLRAClient();
        }

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
