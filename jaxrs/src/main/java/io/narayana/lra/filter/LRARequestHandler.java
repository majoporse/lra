/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.filter;

import static io.narayana.lra.LRAConstants.AFTER;
import static io.narayana.lra.LRAConstants.COMPENSATE;
import static io.narayana.lra.LRAConstants.COMPLETE;
import static io.narayana.lra.LRAConstants.FORGET;
import static io.narayana.lra.LRAConstants.LEAVE;
import static io.narayana.lra.LRAConstants.STATUS;
import static io.narayana.lra.LRAConstants.TIMELIMIT_PARAM_NAME;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;
import static jakarta.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.Type.MANDATORY;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.Type.NESTED;

import io.narayana.lra.AnnotationResolver;
import io.narayana.lra.BearerTokenResolver;
import io.narayana.lra.Current;
import io.narayana.lra.PropagateToken;
import io.narayana.lra.client.CallbackRegistrar;
import io.narayana.lra.client.LRAClient;
import io.narayana.lra.client.NarayanaLRAClient;
import io.narayana.lra.client.internal.proxy.nonjaxrs.LRAParticipant;
import io.narayana.lra.client.internal.proxy.nonjaxrs.LRAParticipantRegistry;
import io.narayana.lra.filter.ProgressTracker.ProgressStep;
import io.narayana.lra.logging.LRALogger;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Link;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.Forget;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.Status;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.eclipse.microprofile.lra.annotation.ws.rs.Leave;

/**
 * Transport-agnostic handler for LRA request processing.
 * Mirrors the logic in {@link ServerLRAFilter} but uses {@link LRARequestContext}
 * and {@link LRAResponseContext} instead of JAX-RS types.
 *
 * <p>
 * This handler can be used by both HTTP (via {@link ServerLRAFilter}) and
 * messaging (via a Kafka interceptor) transports.
 * </p>
 */
public class LRARequestHandler {

    private static final long DEFAULT_TIMEOUT_MILLIS = 0L;

    private final LRAClient lraClient;
    private final CallbackRegistrar callbackRegistrar;
    private final LRAParticipantRegistry lraParticipantRegistry;
    private final int enlistMaxRetries;

    public LRARequestHandler(LRAClient lraClient,
            CallbackRegistrar callbackRegistrar,
            LRAParticipantRegistry lraParticipantRegistry,
            int enlistMaxRetries) {
        this.lraClient = lraClient;
        this.callbackRegistrar = callbackRegistrar;
        this.lraParticipantRegistry = lraParticipantRegistry;
        this.enlistMaxRetries = enlistMaxRetries;
    }

    // =========================================================================
    // Public entry points
    // =========================================================================

    /**
     * Process an incoming request. Dispatches to annotation-specific handlers.
     */
    public void filter(LRARequestContext ctx) {
        Method method = ctx.getResourceMethod();
        propagateAuthToken(ctx);
        URI incomingLRA = ctx.getIncomingLRA();

        // @Leave: leave the incoming LRA (independent of @LRA)
        if (incomingLRA != null && isPresent(Leave.class, method)) {
            handleLeave(ctx, incomingLRA);
            if (ctx.isAborted())
                return;
        }

        // @LRA: transactional processing (start/join/suspend an LRA)
        LRA transactional = resolveTransactionalAnnotation(method);
        if (transactional != null) {
            handleLRA(ctx, transactional, incomingLRA);
            return;
        }

        // @Compensate, @Complete, @Status, @Forget, @AfterLRA: end-annotation methods
        if (isEndAnnotation(method)) {
            handleEndAnnotation(ctx, incomingLRA);
            return;
        }

        // No relevant annotation: non-transactional
        handleNoLRA(ctx, incomingLRA);
    }

    /**
     * Process an outgoing response.
     */
    public void responseFilter(LRARequestContext requestContext, LRAResponseContext responseContext) {
        ProgressTracker tracker = ProgressTracker.from(requestContext.getAbortWith());
        URI suspendedLRA = requestContext.getSuspendedLRA();
        URI current = requestContext.getCurrentLRA();
        URI toClose = requestContext.getTerminalLRA();
        boolean isCancel = responseContext.isCancel();
        String userData = responseContext.getUserData();
        String compensator = requestContext.getCompensatorLink();

        try {
            if (current != null && isCancel) {
                toClose = handleCancelCurrent(requestContext, current, toClose, responseContext, tracker);
            }

            if (toClose != null) {
                handleCloseTerminal(toClose, isCancel, compensator, userData, responseContext, tracker);
            } else if (current != null && compensator != null && userData != null) {
                lraClient.enlistCompensator(current, 0L, compensator, new StringBuilder(userData));
            }

            handleHeaderCleanup(requestContext, responseContext, current, toClose);
            handleAsyncWarning(requestContext, responseContext);
            processFailureReporting(responseContext, tracker);
        } finally {
            cleanupContext(requestContext, responseContext, suspendedLRA, current);
        }
    }

    // =========================================================================
    // @Leave handler
    // =========================================================================

    /**
     * Handle @Leave: leave the incoming LRA.
     */
    private void handleLeave(LRARequestContext ctx, URI incomingLRA) {
        Map<String, String> terminateURIs = callbackRegistrar.getTerminationUris(
                ctx.getResourceClass(),
                ctx.getBaseUri() != null ? ctx.getBaseUri().toString() : "",
                null);
        String compensatorId = terminateURIs.get("Link");

        if (compensatorId == null) {
            abortWith(ctx, incomingLRA.toASCIIString(),
                    Response.Status.BAD_REQUEST.getStatusCode(),
                    "Missing complete or compensate annotations", null);
            return;
        }

        try {
            lraClient.leaveLRA(incomingLRA, compensatorId);
            ctx.setProgress(ProgressStep.Left, null);
        } catch (WebApplicationException e) {
            ctx.setProgress(ProgressStep.LeaveFailed, e.getMessage());
            abortWith(ctx, incomingLRA.toASCIIString(), e.getResponse().getStatus(),
                    e.getMessage(), ctx.getProgressTracker());
        } catch (ProcessingException e) {
            ctx.setProgress(ProgressStep.LeaveFailed, e.getMessage());
            abortWith(ctx, incomingLRA.toASCIIString(),
                    Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
                    e.getMessage(), ctx.getProgressTracker());
        }
    }

    // =========================================================================
    // @LRA handler
    // =========================================================================

    /**
     * Handle @LRA: the main transactional processing.
     * Resolves the annotation properties, determines the LRA type, starts/joins/suspends as needed,
     * and enlists the compensator.
     */
    private void handleLRA(LRARequestContext ctx, LRA transactional, URI incomingLRA) {
        // Resolve annotation properties
        LRA.Type type = transactional.value();
        boolean isLongRunning = !transactional.end();
        Long timeout = null;
        if (transactional.timeLimit() != 0) {
            timeout = Duration.of(transactional.timeLimit(), transactional.timeUnit()).toMillis();
        }
        resolveCancelOnProperties(ctx, transactional);

        boolean endAnnotation = isEndAnnotation(ctx.getResourceMethod());
        ctx.setEndAnnotation(endAnnotation);

        // Set parent context header from incoming LRA
        if (incomingLRA != null) {
            setParentContextHeader(ctx, incomingLRA);
            if (ctx.isAborted())
                return;
        }

        // Re-check for incoming LRA from request properties (nested/async scenarios)
        if (incomingLRA == null) {
            incomingLRA = ctx.getIncomingLRA();
        }

        // Resolve the LRA ID based on the type
        LraResolutionResult result = resolveLRAForType(ctx, type, incomingLRA, timeout);
        if (result == null)
            return; // abortWith was called

        URI lraId = result.lraId;
        URI newLRA = result.newLRA;
        URI suspendedLRA = result.suspendedLRA;
        boolean requiresActiveLRA = result.requiresActiveLRA;

        // If no LRA ID (NEVER, NOT_SUPPORTED), run without a transaction
        if (lraId == null) {
            finishNonTransactional(ctx, suspendedLRA);
            return;
        }

        // Set up the LRA context
        if (!isLongRunning) {
            ctx.setTerminalLRA(lraId);
        }
        Current.push(lraId);

        if (newLRA != null) {
            if (suspendedLRA != null) {
                ctx.setSuspendedLRA(incomingLRA);
            }
            ctx.setNewLRA(newLRA);
        }

        try {
            lraClient.setCurrentLRA(lraId);
        } catch (Exception e) {
            abortWith(ctx, lraId.toASCIIString(), Response.Status.BAD_REQUEST.getStatusCode(),
                    e.getMessage(), ctx.getProgressTracker());
            return;
        }

        // Enlist compensator (unless this is an end-annotation method)
        if (!endAnnotation) {
            enlistCompensator(ctx, lraId, timeout, requiresActiveLRA, type);
            if (ctx.isAborted())
                return;
        }

        ctx.setCurrentLRA(lraId);
        Current.addActiveLRACache(lraId);
    }

    // =========================================================================
    // End-annotation handlers (@Compensate, @Complete, @Status, @Forget, @AfterLRA)
    // =========================================================================

    /**
     * Handle end-annotation methods without @LRA.
     * These methods participate in an existing LRA but don't start/join one themselves.
     * If there's an incoming LRA, preserve it in the context.
     */
    private void handleEndAnnotation(LRARequestContext ctx, URI incomingLRA) {
        ctx.setEndAnnotation(true);

        if (incomingLRA == null) {
            return;
        }

        // Preserve the incoming LRA context for the end-annotation method
        Current.push(incomingLRA);
        ctx.setSuspendedLRA(incomingLRA);
        ctx.setCurrentLRA(incomingLRA);
        Current.addActiveLRACache(incomingLRA);
    }

    /**
     * Handle methods with no relevant LRA annotations.
     * If there's an incoming LRA, pass it through. Otherwise, clear the context.
     */
    private void handleNoLRA(LRARequestContext ctx, URI incomingLRA) {
        Current.popAll();
        ctx.setClearContextHeaders(true);

        if (incomingLRA != null) {
            Current.push(incomingLRA);
            ctx.setSuspendedLRA(incomingLRA);
            ctx.setCurrentLRA(incomingLRA);
            Current.addActiveLRACache(incomingLRA);
        }
    }

    // =========================================================================
    // @LRA sub-handlers (LRA type resolution, enlistment)
    // =========================================================================

    // Result holder for resolveLRAForType
    private static class LraResolutionResult {
        final URI lraId;
        final URI newLRA;
        final URI suspendedLRA;
        final boolean requiresActiveLRA;

        LraResolutionResult(URI lraId, URI newLRA, URI suspendedLRA, boolean requiresActiveLRA) {
            this.lraId = lraId;
            this.newLRA = newLRA;
            this.suspendedLRA = suspendedLRA;
            this.requiresActiveLRA = requiresActiveLRA;
        }
    }

    /**
     * Resolve the LRA ID based on the type and incoming context.
     *
     * @return the resolution result, or null if the request was aborted
     */
    private LraResolutionResult resolveLRAForType(LRARequestContext ctx, LRA.Type type,
            URI incomingLRA, Long timeout) {
        URI lraId;
        URI newLRA = null;
        URI suspendedLRA = null;
        boolean requiresActiveLRA = false;
        Method method = ctx.getResourceMethod();

        switch (type) {
            case MANDATORY:
                if (isTxInvalid(ctx, type, incomingLRA, true))
                    return null;
                lraId = incomingLRA;
                requiresActiveLRA = true;
                break;

            case NEVER:
                if (isTxInvalid(ctx, type, incomingLRA, false))
                    return null;
                lraId = null;
                break;

            case NOT_SUPPORTED:
                suspendedLRA = incomingLRA;
                lraId = null;
                break;

            case NESTED:
                if (incomingLRA != null) {
                    ctx.setParentContextHeader(incomingLRA.toASCIIString());
                    suspendedLRA = incomingLRA;
                }
                // When incomingLRA is null, NESTED behaves like REQUIRED (start a new root LRA)
                // FALLTHROUGH

            case REQUIRED:
                if (incomingLRA != null && type != NESTED) {
                    lraId = incomingLRA;
                    requiresActiveLRA = true;
                } else {
                    URI parentLRA = (type == NESTED) ? incomingLRA : null;
                    newLRA = lraId = startLRA(ctx, parentLRA, method, timeout);
                    if (newLRA == null)
                        return null;
                }
                break;

            case REQUIRES_NEW:
                suspendedLRA = incomingLRA;
                newLRA = lraId = startLRA(ctx, null, method, timeout);
                if (newLRA == null)
                    return null;
                break;

            case SUPPORTS:
                lraId = incomingLRA;
                break;

            default:
                lraId = incomingLRA;
                break;
        }

        return new LraResolutionResult(lraId, newLRA, suspendedLRA, requiresActiveLRA);
    }

    /**
     * Enlist the current resource as a compensator with the coordinator.
     */
    private void enlistCompensator(LRARequestContext ctx, URI lraId, Long timeout,
            boolean requiresActiveLRA, LRA.Type type) {
        Map<String, String> terminateURIs = callbackRegistrar.getTerminationUris(
                ctx.getResourceClass(),
                ctx.getBaseUri() != null ? ctx.getBaseUri().toString() : "",
                timeout);
        String timeLimitStr = terminateURIs.get(TIMELIMIT_PARAM_NAME);
        long timeLimit = timeLimitStr == null ? DEFAULT_TIMEOUT_MILLIS : Long.parseLong(timeLimitStr);

        LRAParticipant participant = lraParticipantRegistry != null
                ? lraParticipantRegistry.getParticipant(ctx.getResourceClass().getName())
                : null;

        if (terminateURIs.containsKey("Link") || participant != null) {
            doEnlistCompensator(ctx, lraId, terminateURIs, participant, timeLimit);
        } else if (requiresActiveLRA && lraClient.getStatus(lraId) != LRAStatus.Active) {
            handleInactiveLRA(ctx, lraId, type);
        }
    }

    private void doEnlistCompensator(LRARequestContext ctx, URI lraId,
            Map<String, String> terminateURIs, LRAParticipant participant, long timeLimit) {
        try {
            if (participant != null) {
                participant.augmentTerminationURIs(terminateURIs, ctx.getRequestBaseUri());
            }

            String compensatorLink = buildCompensatorURI(
                    toURI(terminateURIs.get(COMPENSATE)),
                    toURI(terminateURIs.get(COMPLETE)),
                    toURI(terminateURIs.get(FORGET)),
                    toURI(terminateURIs.get(LEAVE)),
                    toURI(terminateURIs.get(AFTER)),
                    toURI(terminateURIs.get(STATUS)));
            StringBuilder previousParticipantData = new StringBuilder();

            ctx.setCompensatorLink(compensatorLink);

            URI recoveryUrl = enlistWithRetry(lraId, timeLimit, compensatorLink, previousParticipantData);

            if (previousParticipantData.length() != 0) {
                ctx.setPreviousParticipantData(previousParticipantData.toString());
            }

            ctx.setProgress(ProgressStep.Joined, null);
            ctx.setRecoveryUrl(recoveryUrl);
        } catch (WebApplicationException e) {
            String reason = e.getResponse().readEntity(String.class);
            ctx.setProgress(ProgressStep.JoinFailed, reason);
            abortWith(ctx, lraId.toASCIIString(), e.getResponse().getStatus(),
                    String.format("%s: %s", e.getClass().getSimpleName(), reason), ctx.getProgressTracker());
        } catch (URISyntaxException e) {
            ctx.setProgress(ProgressStep.JoinFailed, e.getMessage());
            abortWith(ctx, lraId.toASCIIString(), Response.Status.BAD_REQUEST.getStatusCode(),
                    String.format("%s %s: %s", lraId, e.getClass().getSimpleName(), e.getMessage()), ctx.getProgressTracker());
        } catch (ProcessingException e) {
            ctx.setProgress(ProgressStep.JoinFailed, e.getMessage());
            abortWith(ctx, lraId.toASCIIString(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
                    String.format("%s %s,", e.getClass().getSimpleName(), e.getMessage()), ctx.getProgressTracker());
        }
    }

    private URI enlistWithRetry(URI lraId, long timeLimit, String compensatorLink,
            StringBuilder previousParticipantData) throws WebApplicationException {
        for (int i = 0;; i++) {
            try {
                return lraClient.enlistCompensator(lraId, timeLimit, compensatorLink, previousParticipantData);
            } catch (WebApplicationException e) {
                String logMessage = "The coordinator service is unavailable; failing in enlisting the compensator for "
                        + lraId
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

    // =========================================================================
    // responseFilter() sub-methods
    // =========================================================================

    private URI handleCancelCurrent(LRARequestContext requestContext, URI current,
            URI toClose, LRAResponseContext responseContext, ProgressTracker tracker) {
        try {
            if (tracker == null || tracker.doesNotContain(ProgressStep.StartFailed)) {
                lraClient.cancelLRA(current);
                if (tracker != null)
                    tracker.add(ProgressStep.Ended, null);
            }
        } catch (NotFoundException ignore) {
            if (tracker != null)
                tracker.add(ProgressStep.Ended, null);
        } catch (WebApplicationException e) {
            if (tracker != null)
                tracker.add(ProgressStep.CancelFailed, e.getMessage());
        } catch (ProcessingException e) {
            Method method = requestContext.getResourceMethod();
            LRALogger.i18nLogger.warn_lraFilterContainerRequest("ProcessingException: " + e.getMessage(),
                    method.getDeclaringClass().getName() + "#" + method.getName(), current.toASCIIString());
            if (tracker != null)
                tracker.add(ProgressStep.CancelFailed, e.getMessage());
            toClose = null;
        } finally {
            responseContext.setCancelledCurrent(true);
            if (toClose != null && toClose.toASCIIString().equals(current.toASCIIString())) {
                toClose = null;
            }
        }
        return toClose;
    }

    private void handleCloseTerminal(URI toClose, boolean isCancel, String compensator,
            String userData, LRAResponseContext responseContext, ProgressTracker tracker) {
        responseContext.setClosedTerminal(true);
        try {
            if (tracker == null || tracker.doesNotContain(ProgressStep.StartFailed)) {
                if (isCancel) {
                    lraClient.cancelLRA(toClose, compensator, userData);
                } else {
                    lraClient.closeLRA(toClose, compensator, userData);
                }
                if (tracker != null)
                    tracker.add(ProgressStep.Ended, null);
            }
        } catch (WebApplicationException e) {
            if (e.getResponse().getStatus() == NOT_FOUND.getStatusCode()) {
                if (tracker != null)
                    tracker.add(ProgressStep.Ended, null);
            } else if (tracker != null) {
                tracker.add(isCancel ? ProgressStep.CancelFailed : ProgressStep.CloseFailed, e.getMessage());
            }
        } catch (ProcessingException e) {
            if (tracker != null) {
                tracker.add(isCancel ? ProgressStep.CancelFailed : ProgressStep.CloseFailed, e.getMessage());
            }
        }
    }

    private void handleHeaderCleanup(LRARequestContext requestContext, LRAResponseContext responseContext,
            URI current, URI toClose) {
        if (responseContext.isCancelledCurrent()
                && current != null
                && current.toASCIIString()
                        .equals(Current.getLast(requestContext.getHeaders().get(LRA_HTTP_CONTEXT_HEADER)))) {
            requestContext.getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);
        }

        if (responseContext.isClosedTerminal()) {
            requestContext.getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);
            if (toClose != null && toClose.toASCIIString().equals(
                    Current.getLast(requestContext.getHeaders().get(LRA_HTTP_CONTEXT_HEADER)))) {
                requestContext.getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);
            }
        }
    }

    private void handleAsyncWarning(LRARequestContext requestContext, LRAResponseContext responseContext) {
        if (responseContext.getResponseStatus() == Response.Status.OK.getStatusCode()
                && requestContext.getResourceMethod() != null
                && NarayanaLRAClient.isAsyncCompletion(requestContext.getResourceMethod())) {
            LRALogger.i18nLogger.warn_lraParticipantqForAsync(
                    requestContext.getResourceMethod().getDeclaringClass().getName(),
                    requestContext.getResourceMethod().getName(),
                    Response.Status.ACCEPTED.getStatusCode(),
                    Response.Status.OK.getStatusCode());
        }
    }

    private void processFailureReporting(LRAResponseContext responseContext, ProgressTracker tracker) {
        if (tracker != null) {
            String failureMessage = tracker.buildFailureMessage();
            if (failureMessage != null) {
                responseContext.setFailureMessage(failureMessage);
                LRALogger.logger.warn(failureMessage);
            }
        }
    }

    private void cleanupContext(LRARequestContext requestContext, LRAResponseContext responseContext,
            URI suspendedLRA, URI current) {
        if (suspendedLRA != null) {
            Current.push(suspendedLRA);
        }
        responseContext.setLraContexts(Current.getContexts());
        Current.popAll();
        Current.removeActiveLRACache(current);
        Current.clearAuthToken();
    }

    // =========================================================================
    // Shared helpers
    // =========================================================================

    private LRA resolveTransactionalAnnotation(Method method) {
        LRA transactional = AnnotationResolver.resolveAnnotation(LRA.class, method);
        if (transactional == null) {
            transactional = method.getDeclaringClass().getDeclaredAnnotation(LRA.class);
        }
        return transactional;
    }

    private void propagateAuthToken(LRARequestContext ctx) {
        Method method = ctx.getResourceMethod();
        if (!isPresent(PropagateToken.class, method)
                && !method.getDeclaringClass().isAnnotationPresent(PropagateToken.class)) {
            return;
        }
        String authHeader = ctx.getAuthorizationHeader();
        if (authHeader != null && authHeader.regionMatches(true, 0, "Bearer ", 0, 7)) {
            String token = authHeader.substring(7);
            if (BearerTokenResolver.isPlausibleJwt(token)) {
                Current.setAuthToken(token);
            } else {
                LRALogger.logger.warnf("@PropagateToken: Authorization header does not contain a valid JWT structure");
            }
        }
    }

    private boolean isEndAnnotation(Method method) {
        return isPresent(Complete.class, method)
                || isPresent(Compensate.class, method)
                || isPresent(Leave.class, method)
                || isPresent(Status.class, method)
                || isPresent(Forget.class, method)
                || isPresent(AfterLRA.class, method);
    }

    private boolean isPresent(Class<? extends Annotation> annotation, Method method) {
        return AnnotationResolver.isAnnotationPresent(annotation, method);
    }

    private void resolveCancelOnProperties(LRARequestContext ctx, LRA transactional) {
        Response.Status.Family[] cancelOnFamily = transactional.cancelOnFamily();
        Response.Status[] cancelOn = transactional.cancelOn();
        if (cancelOnFamily.length != 0)
            ctx.setCancelOnFamily(cancelOnFamily);
        if (cancelOn.length != 0)
            ctx.setCancelOn(cancelOn);
    }

    private void setParentContextHeader(LRARequestContext ctx, URI incomingLRA) {
        try {
            ctx.setParentContextHeader(Current.getFirstParent(incomingLRA));
        } catch (UnsupportedEncodingException e) {
            abortWith(ctx, incomingLRA.toASCIIString(),
                    Response.Status.PRECONDITION_FAILED.getStatusCode(),
                    String.format("incoming LRA %s contains an invalid parent: %s", incomingLRA, e.getMessage()),
                    ctx.getProgressTracker());
        }
    }

    private boolean isTxInvalid(LRARequestContext ctx, LRA.Type type, URI lraId, boolean shouldNotBeNull) {
        if (lraId == null && shouldNotBeNull) {
            abortWith(ctx, null, Response.Status.PRECONDITION_FAILED.getStatusCode(),
                    type.name() + " but no tx", ctx.getProgressTracker());
            return true;
        } else if (lraId != null && !shouldNotBeNull) {
            abortWith(ctx, lraId.toASCIIString(), Response.Status.PRECONDITION_FAILED.getStatusCode(),
                    type.name() + " but found tx", ctx.getProgressTracker());
            return true;
        }
        return false;
    }

    private void handleInactiveLRA(LRARequestContext ctx, URI lraId, LRA.Type type) {
        Current.popAll();
        Current.pop(lraId);
        ctx.setClearContextHeaders(true);
        ctx.setSuspendedLRA(null);
        if (type == MANDATORY) {
            abortWith(ctx, lraId.toASCIIString(),
                    Response.Status.PRECONDITION_FAILED.getStatusCode(),
                    "LRA should have been active: ", ctx.getProgressTracker());
        }
    }

    private void finishNonTransactional(LRARequestContext ctx, URI suspendedLRA) {
        Current.popAll();
        ctx.setClearContextHeaders(true);
        if (suspendedLRA != null) {
            ctx.setSuspendedLRA(suspendedLRA);
        }
    }

    private URI startLRA(LRARequestContext ctx, URI parentLRA, Method method, Long timeout) {
        String clientId = method.getDeclaringClass().getName() + "#" + method.getName();
        try {
            URI lra = lraClient.startLRA(parentLRA, clientId, timeout, ChronoUnit.MILLIS, false);
            ctx.setProgress(ProgressStep.Started, null);
            return lra;
        } catch (WebApplicationException e) {
            String msg = e.getResponse().readEntity(String.class);
            ctx.setProgress(ProgressStep.StartFailed, msg);
            abortWith(ctx, null, e.getResponse().getStatus(), msg, ctx.getProgressTracker());
        }
        return null;
    }

    private void abortWith(LRARequestContext ctx, String lraId, int statusCode,
            String message, ProgressTracker tracker) {
        ctx.setAborted(true);
        ctx.setAbortStatusCode(statusCode);
        ctx.setAbortMessage(message);
        ctx.setAbortWith(tracker);
        Method method = ctx.getResourceMethod();
        LRALogger.i18nLogger.warn_lraFilterContainerRequest(message,
                method.getDeclaringClass().getName() + "#" + method.getName(),
                lraId == null ? "context" : lraId);
    }

    private URI toURI(String uri) throws URISyntaxException {
        return uri == null ? null : new URI(uri);
    }

    private String buildCompensatorURI(URI compensate, URI complete, URI forget,
            URI leave, URI after, URI status) {
        StringBuilder b = new StringBuilder();
        makeLink(b, COMPENSATE, compensate);
        makeLink(b, COMPLETE, complete);
        makeLink(b, FORGET, forget);
        makeLink(b, LEAVE, leave);
        makeLink(b, AFTER, after);
        makeLink(b, STATUS, status);
        return b.toString();
    }

    private static void makeLink(StringBuilder b, String key, URI value) {
        if (key == null || value == null)
            return;
        String uri = value.toASCIIString();
        Link link = Link.fromUri(uri).title(key + " URI").rel(key).type(MediaType.TEXT_PLAIN).build();
        if (b.length() != 0)
            b.append(',');
        b.append(link);
    }
}
