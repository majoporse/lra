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
import io.narayana.lra.client.NarayanaLRAClient;
import io.narayana.lra.client.internal.proxy.nonjaxrs.LRAParticipant;
import io.narayana.lra.client.internal.proxy.nonjaxrs.LRAParticipantRegistry;
import io.narayana.lra.logging.LRALogger;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Link;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.StringJoiner;
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

    private final NarayanaLRAClient lraClient;
    private final CallbackRegistrar callbackRegistrar;
    private final LRAParticipantRegistry lraParticipantRegistry;
    private final int enlistMaxRetries;

    public LRARequestHandler(NarayanaLRAClient lraClient,
            CallbackRegistrar callbackRegistrar,
            LRAParticipantRegistry lraParticipantRegistry,
            int enlistMaxRetries) {
        this.lraClient = lraClient;
        this.callbackRegistrar = callbackRegistrar;
        this.lraParticipantRegistry = lraParticipantRegistry;
        this.enlistMaxRetries = enlistMaxRetries;
    }

    /**
     * Process an incoming request. Mirrors {@link ServerLRAFilter#filter(jakarta.ws.rs.container.ContainerRequestContext)}.
     *
     * @param containerRequestContext the transport-agnostic request context
     */
    public void filter(LRARequestContext containerRequestContext) {
        // Note that this filter uses abortWith instead of throwing exceptions on encountering exceptional
        // conditions. This facilitates async because filters for asynchronous JAX-RS methods are
        // not allowed to throw exceptions.
        Method method = containerRequestContext.getResourceMethod();
        LRA.Type type = null;
        LRA transactional = AnnotationResolver.resolveAnnotation(LRA.class, method);
        URI lraId;
        URI newLRA = null;
        Long timeout = null;

        URI suspendedLRA = null;
        URI incomingLRA = containerRequestContext.getIncomingLRA();
        URI recoveryUrl;
        boolean isLongRunning = false;
        boolean requiresActiveLRA = false;
        ArrayList<Progress> progress = null;

        if (transactional == null) {
            transactional = method.getDeclaringClass().getDeclaredAnnotation(LRA.class);
        }

        if (AnnotationResolver.isAnnotationPresent(PropagateToken.class, method)
                || method.getDeclaringClass().isAnnotationPresent(PropagateToken.class)) {
            String authHeader = containerRequestContext.getAuthorizationHeader();
            if (authHeader != null && authHeader.regionMatches(true, 0, "Bearer ", 0, 7)) {
                String token = authHeader.substring(7);
                if (BearerTokenResolver.isPlausibleJwt(token)) {
                    Current.setAuthToken(token);
                } else {
                    LRALogger.logger.warnf("@PropagateToken: Authorization header does not contain a valid JWT structure");
                }
            }
        }

        if (transactional != null) {
            type = transactional.value();
            isLongRunning = !transactional.end();
            Response.Status.Family[] cancel0nFamily = transactional.cancelOnFamily();
            Response.Status[] cancel0n = transactional.cancelOn();

            if (cancel0nFamily.length != 0) {
                containerRequestContext.setCancelOnFamily(cancel0nFamily);
            }

            if (cancel0n.length != 0) {
                containerRequestContext.setCancelOn(cancel0n);
            }

            if (transactional.timeLimit() != 0) {
                timeout = Duration.of(transactional.timeLimit(), transactional.timeUnit()).toMillis();
            }
        }

        boolean endAnnotation = AnnotationResolver.isAnnotationPresent(Complete.class, method)
                || AnnotationResolver.isAnnotationPresent(Compensate.class, method)
                || AnnotationResolver.isAnnotationPresent(Leave.class, method)
                || AnnotationResolver.isAnnotationPresent(Status.class, method)
                || AnnotationResolver.isAnnotationPresent(Forget.class, method)
                || AnnotationResolver.isAnnotationPresent(AfterLRA.class, method);

        containerRequestContext.setEndAnnotation(endAnnotation);

        if (incomingLRA != null) {
            if (AnnotationResolver.isAnnotationPresent(Leave.class, method)) {
                // leave the LRA
                Map<String, String> terminateURIs = callbackRegistrar.getTerminationUris(
                        containerRequestContext.getResourceClass(),
                        containerRequestContext.getBaseUri() != null ? containerRequestContext.getBaseUri().toString() : "",
                        timeout);
                String compensatorId = terminateURIs.get("Link");

                if (compensatorId == null) {
                    abortWith(containerRequestContext, incomingLRA.toASCIIString(),
                            Response.Status.BAD_REQUEST.getStatusCode(),
                            "Missing complete or compensate annotations", null);
                    return; // user error, bail out
                }

                progress = new ArrayList<>();

                try {
                    lraClient.leaveLRA(incomingLRA, compensatorId);
                    progress = updateProgress(progress, ProgressStep.Left, null); // leave succeeded
                } catch (WebApplicationException e) {
                    progress = updateProgress(progress, ProgressStep.LeaveFailed, e.getMessage()); // leave may have failed
                    abortWith(containerRequestContext, incomingLRA.toASCIIString(),
                            e.getResponse().getStatus(),
                            e.getMessage(), progress);
                    return; // the error will be handled or reported via the response filter
                } catch (ProcessingException e) { // a remote coordinator was unavailable
                    progress = updateProgress(progress, ProgressStep.LeaveFailed, e.getMessage()); // leave may have failed
                    abortWith(containerRequestContext, incomingLRA.toASCIIString(),
                            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
                            e.getMessage(), progress);
                    return; // the error will be handled or reported via the response filter
                }

                // let the participant know which lra he left by leaving the header intact
            }
        }

        if (type == null) {
            if (!endAnnotation) {
                Current.popAll();
                containerRequestContext.setClearContextHeaders(true);
            }

            if (incomingLRA != null) {
                Current.push(incomingLRA);
                containerRequestContext.setSuspendedLRA(incomingLRA);
                containerRequestContext.setCurrentLRA(incomingLRA);
                Current.addActiveLRACache(incomingLRA);
            }

            return; // not transactional
        }

        // check the incoming request for an LRA context (for HTTP: check request properties)
        if (incomingLRA == null) {
            // For HTTP: containerRequestContext.getProperty(LRA_HTTP_CONTEXT_HEADER)
            // For messaging: already set from envelope
            incomingLRA = containerRequestContext.getIncomingLRA();
        }

        if (endAnnotation && incomingLRA == null) {
            return;
        }

        if (incomingLRA != null) {
            // set the parent context header
            try {
                String parentId = Current.getFirstParent(incomingLRA);
                containerRequestContext.setParentContextHeader(parentId);
            } catch (UnsupportedEncodingException e) {
                abortWith(containerRequestContext, incomingLRA.toASCIIString(),
                        Response.Status.PRECONDITION_FAILED.getStatusCode(),
                        String.format("incoming LRA %s contains an invalid parent: %s", incomingLRA, e.getMessage()),
                        progress);
                return; // any previous actions (the leave request) will be reported via the response filter
            }
        }

        switch (type) {
            case MANDATORY: // a txn must be present
                if (isTxInvalid(containerRequestContext, type, incomingLRA, true, progress)) {
                    // isTxInvalid will have called abortWith (thus aborting the rest of the filter chain)
                    return; // any previous actions (e.g. the leave request) will be reported via the response filter
                }

                lraId = incomingLRA;
                requiresActiveLRA = true;

                break;
            case NEVER: // a txn must not be present
                if (isTxInvalid(containerRequestContext, type, incomingLRA, false, progress)) {
                    // isTxInvalid will have called abortWith (thus aborting the rest of the filter chain)
                    return; // any previous actions (the leave request) will be reported via the response filter
                }

                lraId = null; // must not run with any context

                break;
            case NOT_SUPPORTED:
                suspendedLRA = incomingLRA;
                lraId = null; // must not run with any context

                break;
            case NESTED:
                // FALLTHROUGH
            case REQUIRED:
                if (incomingLRA != null) {
                    if (type == NESTED) {
                        // set the parent context header
                        containerRequestContext.setParentContextHeader(incomingLRA.toASCIIString());

                        // if there is an LRA present nest a new LRA under it
                        suspendedLRA = incomingLRA;

                        if (progress == null) {
                            progress = new ArrayList<>();
                        }

                        newLRA = lraId = startLRA(containerRequestContext, incomingLRA, method, timeout, progress);

                        if (newLRA == null) {
                            // startLRA will have called abortWith on the request context
                            // the failure plus any previous actions (the leave request) will be reported via the response filter
                            return;
                        }
                    } else {
                        lraId = incomingLRA;
                        // incomingLRA will be resumed
                        requiresActiveLRA = true;
                    }

                } else {
                    progress = new ArrayList<>();
                    newLRA = lraId = startLRA(containerRequestContext, null, method, timeout, progress);

                    if (newLRA == null) {
                        // startLRA will have called abortWith on the request context
                        // the failure and any previous actions (the leave request) will be reported via the response filter
                        return;
                    }
                }

                break;
            case REQUIRES_NEW:
                //                    previous = AtomicAction.suspend();
                suspendedLRA = incomingLRA;

                if (progress == null) {
                    progress = new ArrayList<>();
                }
                newLRA = lraId = startLRA(containerRequestContext, null, method, timeout, progress);

                if (newLRA == null) {
                    // startLRA will have called abortWith on the request context
                    // the failure and any previous actions (the leave request) will be reported via the response filter
                    return;
                }

                break;
            case SUPPORTS:
                lraId = incomingLRA;

                // incomingLRA will be resumed if not null

                break;
            default:
                lraId = incomingLRA;
        }

        if (lraId == null) {
            // the method call needs to run without a transaction
            Current.popAll();
            containerRequestContext.setClearContextHeaders(true);

            if (suspendedLRA != null) {
                containerRequestContext.setSuspendedLRA(suspendedLRA);
            }

            return; // non transactional
        }

        if (!isLongRunning) {
            containerRequestContext.setTerminalLRA(lraId);
        }

        // store state with the current thread
        Current.push(lraId);

        if (newLRA != null) {
            if (suspendedLRA != null) {
                containerRequestContext.setSuspendedLRA(incomingLRA);
            }

            containerRequestContext.setNewLRA(newLRA);
        }

        try {
            lraClient.setCurrentLRA(lraId); // make the current LRA available to the called method
        } catch (Exception e) {
            // should not happen since lraId has already been validated
            // (perhaps we should not use the client API to set the context)
            abortWith(containerRequestContext, lraId.toASCIIString(),
                    Response.Status.BAD_REQUEST.getStatusCode(),
                    e.getMessage(),
                    progress);
            return; // any previous actions (such as leave and start requests) will be reported via the response filter
        }

        if (!endAnnotation) { // don't enlist for methods marked with Compensate, Complete or Leave
            Map<String, String> terminateURIs = callbackRegistrar.getTerminationUris(
                    containerRequestContext.getResourceClass(),
                    containerRequestContext.getBaseUri() != null ? containerRequestContext.getBaseUri().toString() : "",
                    timeout);
            String timeLimitStr = terminateURIs.get(TIMELIMIT_PARAM_NAME);
            long timeLimit = timeLimitStr == null ? DEFAULT_TIMEOUT_MILLIS : Long.parseLong(timeLimitStr);

            LRAParticipant participant = lraParticipantRegistry != null
                    ? lraParticipantRegistry.getParticipant(containerRequestContext.getResourceClass().getName())
                    : null;

            if (terminateURIs.containsKey("Link") || participant != null) {
                try {
                    if (participant != null) {
                        participant.augmentTerminationURIs(terminateURIs, containerRequestContext.getRequestBaseUri());
                    }

                    String compensatorLink = buildCompensatorURI(
                            toURI(terminateURIs.get(COMPENSATE)),
                            toURI(terminateURIs.get(COMPLETE)),
                            toURI(terminateURIs.get(FORGET)),
                            toURI(terminateURIs.get(LEAVE)),
                            toURI(terminateURIs.get(AFTER)),
                            toURI(terminateURIs.get(STATUS)));
                    StringBuilder previousParticipantData = new StringBuilder();

                    // store the registration link in case the participant wants to associate data with the enlistment in the LRA
                    containerRequestContext.setCompensatorLink(compensatorLink);

                    // The coordinator needs to hold a lock to enlist an participant. This lock is only waited on for a small amount of
                    // time (potentially even zero time). This means if multiple participants try to enlist at the same time, enlistment
                    // may fail. We therefore re-try a configurable amount of times.
                    for (int i = 0;; i++) {
                        try {
                            recoveryUrl = lraClient.enlistCompensator(lraId, timeLimit, compensatorLink,
                                    previousParticipantData);
                            break;
                        } catch (WebApplicationException e) {

                            String logMessage = "The coordinator service is unavailable; failing in enlisting the compensator for "
                                    + lraId +
                                    ", ENLIST_PARTICIPANT_CLIENT_MAX_RETRY (via MP config property \"lra.participant.client.max.retry\") set to "
                                    +
                                    enlistMaxRetries + ". ";

                            if (e.getResponse().getStatus() != SERVICE_UNAVAILABLE.getStatusCode() || i >= enlistMaxRetries) {

                                LRALogger.logger.warn(logMessage + "Not retrying.");

                                throw e;
                            }

                            LRALogger.logger.warn(logMessage + " Attempt " + (i + 1) + " of " + enlistMaxRetries + ".");
                        }
                    }

                    if (previousParticipantData.length() != 0) {
                        // this participant has previously updated the LRAParticipantData bean so make it available for this invocation
                        containerRequestContext.setPreviousParticipantData(previousParticipantData.toString());
                    }

                    progress = updateProgress(progress, ProgressStep.Joined, null);

                    // Store recovery URL for the transport layer to write to response headers
                    containerRequestContext.setRecoveryUrl(recoveryUrl);
                } catch (WebApplicationException e) {
                    String reason = e.getResponse().readEntity(String.class);

                    progress = updateProgress(progress, ProgressStep.JoinFailed, reason);
                    abortWith(containerRequestContext, lraId.toASCIIString(),
                            e.getResponse().getStatus(),
                            String.format("%s: %s", e.getClass().getSimpleName(), reason), progress);
                    // the failure plus any previous actions (such as leave and start requests) will be reported via the response filter
                } catch (URISyntaxException e) {
                    progress = updateProgress(progress, ProgressStep.JoinFailed, e.getMessage()); // one or more of the participant end points was invalid
                    abortWith(containerRequestContext, lraId.toASCIIString(),
                            Response.Status.BAD_REQUEST.getStatusCode(),
                            String.format("%s %s: %s", lraId, e.getClass().getSimpleName(), e.getMessage()), progress);
                    // the failure plus any previous actions (such as leave and start requests) will be reported via the response filter
                } catch (ProcessingException e) {
                    progress = updateProgress(progress, ProgressStep.JoinFailed, e.getMessage()); // a remote coordinator was unavailable
                    abortWith(containerRequestContext, lraId.toASCIIString(),
                            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
                            String.format("%s %s,", e.getClass().getSimpleName(), e.getMessage()), progress);
                    // the failure plus any previous actions (such as leave and start requests) will be reported via the response filter
                }
            } else if (requiresActiveLRA && lraClient.getStatus(lraId) != LRAStatus.Active) {
                Current.popAll();
                Current.pop(lraId);
                containerRequestContext.setClearContextHeaders(true);
                containerRequestContext.setSuspendedLRA(null);

                if (type == MANDATORY) {
                    abortWith(containerRequestContext, lraId.toASCIIString(),
                            Response.Status.PRECONDITION_FAILED.getStatusCode(),
                            "LRA should have been active: ", progress);
                    // any previous actions (such as leave and start requests) will be reported via the response filter
                }
            }
        }

        containerRequestContext.setCurrentLRA(lraId);
        Current.addActiveLRACache(lraId);
    }

    /**
     * Process an outgoing response. Mirrors
     * {@link ServerLRAFilter#filter(jakarta.ws.rs.container.ContainerRequestContext, jakarta.ws.rs.container.ContainerResponseContext)}.
     *
     * @param requestContext the transport-agnostic request context
     * @param responseContext the transport-agnostic response context
     */
    public void responseFilter(LRARequestContext requestContext, LRAResponseContext responseContext) {
        // a request is leaving the container so clear any context on the thread and fix up the LRA response header
        ArrayList<Progress> progress = cast(requestContext.getAbortWith());
        Object suspendedLRA = requestContext.getSuspendedLRA();
        URI current = requestContext.getCurrentLRA();
        URI toClose = requestContext.getTerminalLRA();
        boolean isCancel = responseContext.isCancel();
        // the service method has finished but the user data may have changed
        String userData = responseContext.getUserData();
        String compensator = requestContext.getCompensatorLink();

        try {
            if (current != null && isCancel) {
                try {
                    // do not attempt to cancel if the request filter tried but failed to start a new LRA
                    if (progress == null || progressDoesNotContain(progress, ProgressStep.StartFailed)) {
                        lraClient.cancelLRA(current);
                        progress = updateProgress(progress, ProgressStep.Ended, null);
                    }
                } catch (NotFoundException ignore) {
                    // must already be cancelled (if the intercepted method caused it to cancel)
                    // or completed (if the intercepted method caused it to complete)
                    progress = updateProgress(progress, ProgressStep.Ended, null);
                } catch (WebApplicationException e) {
                    progress = updateProgress(progress, ProgressStep.CancelFailed, e.getMessage());
                } catch (ProcessingException e) {
                    Method method = requestContext.getResourceMethod();
                    LRALogger.i18nLogger.warn_lraFilterContainerRequest("ProcessingException: " + e.getMessage(),
                            method.getDeclaringClass().getName() + "#" + method.getName(), current.toASCIIString());

                    progress = updateProgress(progress, ProgressStep.CancelFailed, e.getMessage());
                    toClose = null;
                } finally {
                    responseContext.setCancelledCurrent(true);

                    if (toClose != null && toClose.toASCIIString().equals(current.toASCIIString())) {
                        toClose = null; // don't attempt to finish the LRA twice
                    }
                }
            }

            if (toClose != null) {
                responseContext.setClosedTerminal(true);

                try {
                    // do not attempt to close or cancel if the request filter tried but failed to start a new LRA
                    if (progress == null || progressDoesNotContain(progress, ProgressStep.StartFailed)) {
                        if (isCancel) {
                            lraClient.cancelLRA(toClose, compensator, userData);
                        } else {
                            lraClient.closeLRA(toClose, compensator, userData);
                        }

                        progress = updateProgress(progress, ProgressStep.Ended, null);
                    }
                } catch (WebApplicationException e) {
                    if (e.getResponse().getStatus() == NOT_FOUND.getStatusCode()) {
                        // must already be cancelled (if the intercepted method caused it to cancel)
                        // or completed (if the intercepted method caused it to complete
                        progress = updateProgress(progress, ProgressStep.Ended, null);
                    } else {
                        // same as ProcessingException case
                        progress = updateProgress(progress,
                                isCancel ? ProgressStep.CancelFailed : ProgressStep.CloseFailed, e.getMessage());
                    }
                } catch (ProcessingException e) {
                    progress = updateProgress(progress,
                            isCancel ? ProgressStep.CancelFailed : ProgressStep.CloseFailed, e.getMessage());
                }
            } else if (current != null && compensator != null && userData != null) {
                lraClient.enlistCompensator(current, 0L, compensator, new StringBuilder(userData));
            }

            if (responseContext.isCancelledCurrent()
                    && current != null
                    && current.toASCIIString()
                            .equals(Current.getLast(requestContext.getHeaders().get(LRA_HTTP_CONTEXT_HEADER)))) {
                // the callers context was ended so invalidate it
                requestContext.getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);
            }

            if (responseContext.isClosedTerminal()) {
                requestContext.getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);

                if (toClose != null && toClose.toASCIIString().equals(
                        Current.getLast(requestContext.getHeaders().get(LRA_HTTP_CONTEXT_HEADER)))) {
                    // the callers context was ended so invalidate it
                    requestContext.getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);
                }
            }

            if (responseContext.getResponseStatus() == Response.Status.OK.getStatusCode()
                    && requestContext.getResourceMethod() != null
                    && NarayanaLRAClient.isAsyncCompletion(requestContext.getResourceMethod())) {
                LRALogger.i18nLogger.warn_lraParticipantqForAsync(
                        requestContext.getResourceMethod().getDeclaringClass().getName(),
                        requestContext.getResourceMethod().getName(),
                        Response.Status.ACCEPTED.getStatusCode(),
                        Response.Status.OK.getStatusCode());
            }

            /*
             * report any failed steps (ie if progress contains any failures) to the caller.
             * If either filter encountered a failure they may have completed partial actions, and
             * we need tell the caller which steps failed and which ones succeeded. We use a
             * different warning code for each scenario:
             */
            if (progress != null) {
                String failureMessage = processLRAOperationFailures(progress);

                if (failureMessage != null) {
                    responseContext.setFailureMessage(failureMessage);
                    LRALogger.logger.warn(failureMessage); // any other failure(s) will already have been logged
                }
            }
        } finally {
            if (suspendedLRA != null) {
                Current.push((URI) suspendedLRA);
            }

            // Capture the LRA context stack before clearing, so the transport layer
            // can write it to response headers (mirrors old code's Current.updateLRAContext)
            responseContext.setLraContexts(Current.getContexts());

            Current.popAll();
            Current.removeActiveLRACache(current);
            Current.clearAuthToken();
        }
    }

    // --- Private helper methods (mirrors ServerLRAFilter) ---

    private boolean isTxInvalid(LRARequestContext ctx, LRA.Type type, URI lraId,
            boolean shouldNotBeNull, ArrayList<Progress> progress) {
        if (lraId == null && shouldNotBeNull) {
            abortWith(ctx, null, Response.Status.PRECONDITION_FAILED.getStatusCode(),
                    type.name() + " but no tx", progress);
            return true;
        } else if (lraId != null && !shouldNotBeNull) {
            abortWith(ctx, lraId.toASCIIString(), Response.Status.PRECONDITION_FAILED.getStatusCode(),
                    type.name() + " but found tx", progress);
            return true;
        }

        return false;
    }

    private URI startLRA(LRARequestContext ctx, URI parentLRA, Method method, Long timeout,
            ArrayList<Progress> progress) {
        // timeout should already have been converted to milliseconds
        String clientId = method.getDeclaringClass().getName() + "#" + method.getName();

        try {
            URI lra = lraClient.startLRA(parentLRA, clientId, timeout, ChronoUnit.MILLIS, false);
            updateProgress(progress, ProgressStep.Started, null);
            return lra;
        } catch (WebApplicationException e) {
            String msg = e.getResponse().readEntity(String.class);

            updateProgress(progress, ProgressStep.StartFailed, msg);

            abortWith(ctx, null,
                    e.getResponse().getStatus(),
                    msg,
                    progress);
        }

        return null;
    }

    // the processing performed by the request filter caused the request to abort (without executing application code)
    private void abortWith(LRARequestContext ctx, String lraId, int statusCode,
            String message, Collection<Progress> reasons) {
        ctx.setAborted(true);
        ctx.setAbortStatusCode(statusCode);
        ctx.setAbortMessage(message);
        ctx.setAbortWith(reasons); // stored as Object, cast back in responseFilter

        Method method = ctx.getResourceMethod();
        LRALogger.i18nLogger.warn_lraFilterContainerRequest(message,
                method.getDeclaringClass().getName() + "#" + method.getName(),
                lraId == null ? "context" : lraId);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Collection<?>> T cast(Object obj) {
        return (T) obj;
    }

    private URI toURI(String uri) throws URISyntaxException {
        return uri == null ? null : new URI(uri);
    }

    private String buildCompensatorURI(URI compensate, URI complete, URI forget, URI leave, URI after, URI status) {
        StringBuilder linkHeaderValue = new StringBuilder();

        makeLink(linkHeaderValue, COMPENSATE, compensate);
        makeLink(linkHeaderValue, COMPLETE, complete);
        makeLink(linkHeaderValue, FORGET, forget);
        makeLink(linkHeaderValue, LEAVE, leave);
        makeLink(linkHeaderValue, AFTER, after);
        makeLink(linkHeaderValue, STATUS, status);

        return linkHeaderValue.toString();
    }

    private static void makeLink(StringBuilder b, String key, URI value) {
        if (key == null || value == null) {
            return;
        }

        String uri = value.toASCIIString();
        Link link = Link.fromUri(uri).title(key + " URI").rel(key).type(MediaType.TEXT_PLAIN).build();

        if (b.length() != 0) {
            b.append(',');
        }

        b.append(link);
    }

    // the request filter may perform multiple and in failure scenarios the LRA may be left in an ambiguous state:
    // the following structure is used to track progress so that such failures can be reported in the response
    // filter processing
    private enum ProgressStep {
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
    private static class Progress {
        static EnumSet<ProgressStep> failures = EnumSet.of(
                ProgressStep.LeaveFailed,
                ProgressStep.StartFailed,
                ProgressStep.JoinFailed,
                ProgressStep.CloseFailed,
                ProgressStep.CancelFailed);

        ProgressStep progress;
        String reason;

        public Progress(ProgressStep progress, String reason) {
            this.progress = progress;
            this.reason = reason;
        }

        public boolean wasSuccessful() {
            return !failures.contains(progress);
        }
    }

    // convert the list of steps carried out by the filters into a warning message
    // the successful operations are logged at debug and the unsuccessful operations are reported back to the caller.
    // The reason we log multiple failures is that one failure can trigger other operations that may also fail,
    // eg enlisting with an LRA could fail followed by a failure to cancel the LRA
    private String processLRAOperationFailures(ArrayList<Progress> progress) {
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

    private boolean progressDoesNotContain(ArrayList<Progress> progress, ProgressStep step) {
        return progress.stream().noneMatch(p -> p.progress == step);
    }

    // add another step to the list of steps performed so far
    private ArrayList<Progress> updateProgress(ArrayList<Progress> progress, ProgressStep step, String reason) {
        if (reason == null) {
            LRALogger.logger.debug(step.toString());
        } else {

            if (progress == null) {
                progress = new ArrayList<>();
            }

            progress.add(new Progress(step, reason));
        }

        return progress;
    }
}
