/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.domain.model;

import static io.narayana.lra.LRAConstants.AFTER;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;

import com.arjuna.ats.arjuna.common.Uid;
import com.arjuna.ats.arjuna.coordinator.AbstractRecord;
import com.arjuna.ats.arjuna.coordinator.RecordType;
import com.arjuna.ats.arjuna.coordinator.TwoPhaseOutcome;
import com.arjuna.ats.arjuna.state.InputObjectState;
import com.arjuna.ats.arjuna.state.OutputObjectState;
import io.narayana.lra.LRAConstants;
import io.narayana.lra.LRAData;
import io.narayana.lra.coordinator.domain.service.LRAService;
import io.narayana.lra.logging.LRALogger;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.client.AsyncInvoker;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.Link;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

public class LRAParticipantRecord extends AbstractRecord implements Comparable<AbstractRecord> {
    private static final String TYPE_NAME = "/StateManager/AbstractRecord/LRARecord";
    private static final String COMPENSATE_REL = "compensate";
    private static final String COMPLETE_REL = "complete";

    private URI lraId;
    private URI parentId;
    private URI recoveryURI;
    private String participantPath;

    private String completeEndpoint;
    private String compensateEndpoint;
    private String statusEndpoint;
    private String forgetEndpoint;
    private String afterEndpoint;

    private String responseData;
    private String compensatorData;
    private String previousCompensatorData;
    private LRAService lraService;
    private ParticipantStatus status;
    private boolean accepted;
    private LongRunningAction lra;

    public LRAParticipantRecord() {
    }

    LRAParticipantRecord(LongRunningAction lra, LRAService lraService, String linkURI, String compensatorData) {
        super(new Uid());

        this.lra = lra;

        // if compensateURI is a link parse it into compensate,complete and status urls
        if (linkURI.startsWith("<")) {
            Exception[] parseException = { null };

            Arrays.stream(linkURI.split(",")).forEach((linkStr) -> {
                Exception e = parseLink(linkStr);
                if (e != null) {
                    parseException[0] = e;
                }
            });

            if (parseException[0] != null) {
                String errorMsg = LRALogger.i18nLogger.error_invalidCompensator(lra.getId(), parseException[0].getMessage(),
                        linkURI);
                LRALogger.logger.error(errorMsg);
                if (LRALogger.logger.isTraceEnabled()) {
                    trace_progress(errorMsg);
                }
                throw new WebApplicationException(Response.status(BAD_REQUEST)
                        .entity(errorMsg)
                        .build());
            } else if (compensateEndpoint == null && afterEndpoint == null) {
                String errorMsg = LRALogger.i18nLogger.error_missingCompensator(lra.getId(), linkURI);
                LRALogger.logger.error(errorMsg);
                if (LRALogger.logger.isTraceEnabled()) {
                    trace_progress(errorMsg);
                }
                throw new WebApplicationException(Response.status(BAD_REQUEST)
                        .entity(errorMsg)
                        .build());
            }
        } else {
            this.compensateEndpoint = String.format("%s/compensate", linkURI);
            this.completeEndpoint = String.format("%s/complete", linkURI);
            this.statusEndpoint = String.format("%s", linkURI);
            this.forgetEndpoint = String.format("%s", linkURI);
        }

        this.lraId = lra.getId();
        this.parentId = lra.getParentId();
        this.status = ParticipantStatus.Active;

        this.lraService = lraService;
        this.participantPath = linkURI;

        this.recoveryURI = null;
        this.compensatorData = compensatorData;

        if (LRALogger.logger.isTraceEnabled()) {
            trace_progress("created");
        }
    }

    LRAParticipantRecord(LongRunningAction lra, LRAService lraService, RegistrationRequest request, String compensatorData) {
        super(new Uid());

        this.lra = lra;
        this.lraId = lra.getId();
        this.parentId = lra.getParentId();
        this.status = ParticipantStatus.Active;
        this.lraService = lraService;
        this.compensatorData = compensatorData;
        this.recoveryURI = null;

        this.compensateEndpoint = request.getCompensateEndpoint();
        this.completeEndpoint = request.getCompleteEndpoint();
        this.statusEndpoint = request.getStatusEndpoint();
        this.forgetEndpoint = request.getForgetEndpoint();
        this.afterEndpoint = request.getAfterEndpoint();

        this.participantPath = "messaging-registration";

        if (LRALogger.logger.isTraceEnabled()) {
            trace_progress("created from messaging registration");
        }
    }

    void setLRA(LongRunningAction lra) {
        this.lra = lra;
        this.parentId = lra.getParentId();
    }

    String getParticipantPath() {
        return participantPath;
    }

    static String cannonicalForm(String linkStr) throws URISyntaxException {
        if (!linkStr.contains(">;")) {
            return new URI(linkStr).toASCIIString();
        }

        SortedMap<String, String> lm = new TreeMap<>();
        Arrays.stream(linkStr.split(",")).forEach(link -> lm.put(Link.valueOf(link).getRel(), link));
        StringBuilder sb = new StringBuilder();

        lm.forEach((k, v) -> appendLink(sb, v));

        return sb.toString();
    }

    private static void appendLink(StringBuilder b, String value) {
        if (b.length() != 0) {
            b.append(',');
        }

        b.append(value);
    }

    static String extractCompensator(String linkStr) throws URISyntaxException {
        for (String lnk : linkStr.split(",")) {
            Link link;

            try {
                link = Link.valueOf(lnk);
            } catch (IllegalArgumentException e) {
                throw new URISyntaxException(lnk, e.getMessage());
            }

            if (COMPENSATE_REL.equals(link.getRel())) {
                return cannonicalForm(link.getUri().toString());
            }
        }

        return linkStr;
    }

    private static URI cannonicalURI(URI uri) throws URISyntaxException {
        return new URI(uri.getScheme(),
                uri.getUserInfo(),
                uri.getHost(),
                uri.getPort(),
                uri.getPath().replaceAll("//", "/"),
                uri.getQuery(), uri.getFragment());
    }

    private URISyntaxException parseLink(String linkStr) {
        Link link = Link.valueOf(linkStr);
        String rel = link.getRel();

        try {
            URI uri = cannonicalURI(link.getUri());
            String uriStr = uri.toASCIIString();

            if (COMPENSATE_REL.equals(rel)) {
                compensateEndpoint = uriStr;
            } else if (COMPLETE_REL.equals(rel)) {
                completeEndpoint = uriStr;
            } else if ("status".equals(rel)) {
                statusEndpoint = uriStr;
            } else if (AFTER.equals(rel)) {
                afterEndpoint = uriStr;
            } else if ("forget".equals(rel)) {
                forgetEndpoint = uriStr;
            } else if ("participant".equals(rel)) {
                compensateEndpoint = uriStr + "/compensate";
                completeEndpoint = uriStr + "/complete";
                statusEndpoint = uriStr;
                forgetEndpoint = uriStr;
            }

            return null;
        } catch (URISyntaxException e) {
            return e;
        }
    }

    @Override
    public int topLevelPrepare() {
        return TwoPhaseOutcome.PREPARE_OK;
    }

    @Override
    // NB if a participant needs to know
    // if the lra closed then it can ask the io.narayana.lra.coordinator. A 404 status implies:
    // - all compensators completed ok, or
    // - all compensators compensated ok
    // This participant can infer which possibility happened since it will have been told to complete or compensate
    public int topLevelAbort() {
        return doEnd(lra.isCancel());
    }

    @Override
    public int topLevelOnePhaseCommit() {
        return topLevelCommit();
    }

    @Override
    public int topLevelCommit() {
        return doEnd(lra.isCancel());
    }

    private int doEnd(boolean compensate) {
        assert lraService != null;

        ReentrantLock lock = lraService.lockTransaction(lraId);

        try {
            return tryDoEnd(compensate);
        } finally {
            lock.unlock();
        }
    }

    private int tryDoEnd(boolean compensate) {
        String endEndpoint;

        if (LRALogger.logger.isTraceEnabled()) {
            trace_progress("finishing");
        }

        if (isFinished()) {
            return atEnd(status == ParticipantStatus.FailedToComplete || status == ParticipantStatus.FailedToCompensate
                    ? TwoPhaseOutcome.FINISH_ERROR
                    : TwoPhaseOutcome.FINISH_OK);
        }

        if (ParticipantStatus.Compensating.equals(status)) {
            compensate = true;
        }

        if (compensateEndpoint == null) {
            return atEnd(TwoPhaseOutcome.FINISH_OK);
        }

        if (compensate) {
            if (isCompensated()) {
                return atEnd(TwoPhaseOutcome.FINISH_OK); // the participant has already compensated
            }

            endEndpoint = compensateEndpoint; // we are going to ask the participant to compensate
            status = ParticipantStatus.Compensating;
        } else {
            if (isCompelete() || completeEndpoint == null) {
                status = ParticipantStatus.Completed;

                return atEnd(TwoPhaseOutcome.FINISH_OK); // the participant has already completed
            }

            endEndpoint = completeEndpoint; // we are going to ask the participant to complete
            status = ParticipantStatus.Completing;
        }

        // NB trying to compensate when already completed is allowed (for nested LRAs)

        int httpStatus = -1;

        if (accepted) {
            // the participant has previously returned a HTTP 202 Accepted response
            // to indicate that it is in progress in which case the status URI
            // must be valid so try that first for the status
            int twoPhaseOutcome = retryGetEndStatus(endEndpoint, compensate);

            if (twoPhaseOutcome != -1) {
                return atEnd(twoPhaseOutcome);
            }
        } else {
            // Try local invocation first (only for HTTP endpoints)
            if (endEndpoint != null && (endEndpoint.startsWith("http://") || endEndpoint.startsWith("https://"))) {
                try {
                    httpStatus = tryLocalEndInvocation(new URI(endEndpoint));
                } catch (URISyntaxException e) {
                    httpStatus = -1;
                }
            }
        }

        if (httpStatus == -1) {
            // the local invocation was not made so fallback to using the notifier

            if (LRALogger.logger.isTraceEnabled()) {
                trace_progress("notifying participant");
            }

            try {
                ParticipantNotifier notifier = ParticipantNotifierHolder.getForEndpoint(endEndpoint);
                String lraIdStr = lraId != null ? lraId.toASCIIString() : null;
                String parentIdStr = parentId != null ? parentId.toASCIIString() : null;
                String recoveryStr = recoveryURI != null ? recoveryURI.toASCIIString() : null;

                if (compensate) {
                    httpStatus = notifier.notifyCompensate(lraIdStr, parentIdStr, endEndpoint, recoveryStr, compensatorData);
                } else {
                    httpStatus = notifier.notifyComplete(lraIdStr, parentIdStr, endEndpoint, recoveryStr, compensatorData);
                }

                // For messaging notifier, httpStatus is -1 (async delivery)
                if (httpStatus == -1) {
                    // Async delivery — treat as accepted
                    accepted = true;
                } else {
                    accepted = httpStatus == Response.Status.ACCEPTED.getStatusCode();
                }

                if (httpStatus == Response.Status.GONE.getStatusCode()) {
                    updateStatus(compensate);
                    return atEnd(TwoPhaseOutcome.FINISH_OK); // the participant must have finished ok but we lost the response
                }
            } catch (Exception e) {
                if (LRALogger.logger.isInfoEnabled()) {
                    LRALogger.logger.infof("LRAParticipantRecord.doEnd(%s) callback at %s failed for LRA %s (reason: %s)",
                            compensate ? "compensate" : "complete", endEndpoint, lraId, e.getMessage());
                    if (LRALogger.logger.isDebugEnabled()) {
                        LRALogger.logger.debug("LRAParticipantRecord.doEnd stacktrace", e);
                    }
                }
            } finally {
                if (LRALogger.logger.isTraceEnabled()) {
                    trace_progress("notified participant");
                }
            }
        }

        if (responseData != null &&
                httpStatus == Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
            // the body should contain a valid ParticipantStatus
            try {
                return atEnd(reportFailure(compensate, endEndpoint,
                        ParticipantStatus.valueOf(responseData).name()));
            } catch (IllegalArgumentException ignore) {
                // ignore the body and let recovery discover the status of the participant
            }
        }

        if (httpStatus != Response.Status.OK.getStatusCode()
                && httpStatus != Response.Status.NO_CONTENT.getStatusCode()
                && httpStatus != -1
                && !accepted) {
            if (LRALogger.logger.isDebugEnabled()) {
                LRALogger.logger.debugf("LRAParticipantRecord.doEnd put %s failed with status: %d",
                        endEndpoint, httpStatus);
            }

            // recovery will figure out the status via the status url
            status = compensate ? ParticipantStatus.Compensating : ParticipantStatus.Completing;
            accepted = true;
            if (LRALogger.logger.isTraceEnabled()) {
                trace_progress("notify participant failed");
            }
        }

        updateStatus(compensate);

        // if the the request is still in progress (ie accepted is true) let recovery finish it
        return atEnd(accepted ? TwoPhaseOutcome.HEURISTIC_HAZARD : TwoPhaseOutcome.FINISH_OK);
    }

    boolean isFinished() {
        // nested participants must still be able to compensate even if they are closed
        if (compensateEndpoint == null) {
            return afterEndpoint != null;
        }

        switch (status) {
            case Completed:
                return parentId == null; // completed nested LRAs must remain cancellable
            case FailedToComplete:
                return true; // terminal failure - cannot be compensated
            case Compensated:
                /* FALLTHRU */
            case FailedToCompensate:
                return true;
            default:
                return false;
        }
    }

    boolean isFailed() {
        return status == ParticipantStatus.FailedToCompensate || status == ParticipantStatus.FailedToComplete;
    }

    private boolean afterLRARequest(String target, String payload) {
        if (LRALogger.logger.isTraceEnabled()) {
            trace_progress("afterLRARequest");
        }

        ParticipantNotifier notifier = ParticipantNotifierHolder.getForEndpoint(target);
        String lraIdStr = lra.getId() != null ? lra.getId().toASCIIString() : null;
        String parentIdStr = lra.getParentId() != null ? lra.getParentId().toASCIIString() : null;
        String recoveryStr = recoveryURI != null ? recoveryURI.toASCIIString() : null;

        boolean result = notifier.notifyAfterLRA(lraIdStr, parentIdStr, target, recoveryStr, compensatorData, payload);

        if (LRALogger.logger.isTraceEnabled()) {
            trace_progress(result ? "notified participant" : "failed to notify participant");
        }

        return result;
    }

    private int atEnd(int res) {
        if (parentId != null && status == ParticipantStatus.Completed) {
            if (lraService.getLRA(parentId).getStatus() == LRAStatus.Active) {
                // completed nested participants must remain compensatable
                return TwoPhaseOutcome.HEURISTIC_HAZARD; // ask to be called again
            } else {
                // the parent is finishing so this is the post LRA invocation
                return runPostLRAActions();
            }
        }

        // Only run the post LRA actions if both the LRA and participant are in an end state
        // check the participant first since it will have been removed from one of the lists
        if (!isFinished() || !lra.isFinished()) {
            if (afterEndpoint != null) {
                return TwoPhaseOutcome.HEURISTIC_HAZARD;
            }

            return res;
        }

        return runPostLRAActions();
    }

    private int runPostLRAActions() {
        LRAStatus lraStatus = lra.getLRAStatus();
        boolean report = false;

        if (lraStatus == LRAStatus.Cancelling) {
            report = isFailed();
            lraStatus = report ? LRAStatus.FailedToCancel : LRAStatus.Cancelled;
        } else if (lraStatus == LRAStatus.Closing) {
            report = isFailed();
            lraStatus = report ? LRAStatus.FailedToClose : LRAStatus.Closed;
        }

        if (afterEndpoint == null || afterLRARequest(afterEndpoint, lraStatus.name())) {
            afterEndpoint = null;

            if (LRALogger.logger.isTraceEnabled()) {
                trace_progress("runPostLRAActions with afterURI");
            }
            // the post LRA actions succeeded so remove the participant from the intentions list otherwise retry
            return report ? reportFailure(lraStatus.name()) : TwoPhaseOutcome.FINISH_OK;
        }

        if (LRALogger.logger.isTraceEnabled()) {
            trace_progress("runPostLRAActions");
        }

        return report ? reportFailure(lraStatus.name()) : TwoPhaseOutcome.HEURISTIC_HAZARD;
    }

    private void updateStatus(boolean compensate) {
        if (compensate) {
            status = accepted ? ParticipantStatus.Compensating : ParticipantStatus.Compensated;
        } else {
            status = accepted ? ParticipantStatus.Completing : ParticipantStatus.Completed;
        }
    }

    private int reportFailure(String failureReason) {
        if (status == ParticipantStatus.FailedToCompensate) {
            return reportFailure(true, compensateEndpoint, failureReason);
        } else { // must be ParticipantStatus.FailedToComplete
            return reportFailure(false, completeEndpoint, failureReason);
        }
    }

    private int reportFailure(boolean compensate, String endPath, String failureReason) {
        status = compensate ? ParticipantStatus.FailedToCompensate : ParticipantStatus.FailedToComplete;

        if (LRALogger.logger.isTraceEnabled()) {
            trace_progress("reportFailure");
        }
        LRALogger.logger.warnf("LRAParticipantRecord: participant %s reported a failure to %s (cause %s)",
                endPath, compensate ? COMPENSATE_REL : COMPLETE_REL, failureReason);

        // permanently failed so ask recovery to ignore us in the future.
        return TwoPhaseOutcome.FINISH_ERROR;
    }

    private int retryGetEndStatus(String endPath, boolean compensate) {
        assert accepted;

        // the participant has previously returned a HTTP 202 Accepted response so the status URI
        // must be valid - try that first for the status

        // first check that this isn't a nested coordinator running locally
        URI nestedLraId = null;
        try {
            if (endPath != null && (endPath.startsWith("http://") || endPath.startsWith("https://"))) {
                nestedLraId = extractParentLRA(new URI(endPath));
            }
        } catch (URISyntaxException e) {
            // not a valid URI, skip local check
        }

        if (LRALogger.logger.isTraceEnabled()) {
            trace_progress("retryGetEndStatus");
        }

        if (nestedLraId != null && lraService != null) {
            LongRunningAction transaction = lraService.getTransaction(nestedLraId);

            if (transaction != null) {
                LRAStatus cStatus = transaction.getLRAStatus();

                if (LRALogger.logger.isTraceEnabled()) {
                    trace_progress("retryGetEndStatus: local status " + cStatus);
                }

                if (cStatus == null) {
                    LRALogger.logger.warnf(
                            "LRAParticipantRecord.retryGetEndStatus: local LRA %s accepted but has a null status",
                            endPath);
                    return -1; // shouldn't happen since it imples it's still be active - force end to be called
                }

                switch (cStatus) {
                    case Closed:
                    case Cancelled:
                        return TwoPhaseOutcome.FINISH_OK;
                    case Closing:
                    case Cancelling:
                        return TwoPhaseOutcome.HEURISTIC_HAZARD;
                    case FailedToClose:
                    case FailedToCancel:
                        return reportFailure(compensate, endPath, "unknown");
                    default:
                        return TwoPhaseOutcome.HEURISTIC_HAZARD;
                }
            }
        } else if (statusEndpoint != null) {
            // it is a standard participant - check the status via the notifier
            ParticipantNotifier notifier = ParticipantNotifierHolder.getForEndpoint(statusEndpoint);
            String lraIdStr = lraId != null ? lraId.toASCIIString() : null;
            String recoveryStr = recoveryURI != null ? recoveryURI.toASCIIString() : null;

            ParticipantStatus reportedStatus = notifier.queryStatus(lraIdStr, statusEndpoint, recoveryStr, compensatorData);

            if (reportedStatus == null) {
                // Notifier couldn't determine status (e.g., messaging notifier or HTTP failure)
                return TwoPhaseOutcome.HEURISTIC_HAZARD;
            }

            switch (reportedStatus) {
                case Completed:
                case Compensated:
                    status = reportedStatus;
                    return TwoPhaseOutcome.FINISH_OK;
                case Completing:
                case Compensating:
                    // still in progress - make sure recovery keeps retrying it
                    return TwoPhaseOutcome.HEURISTIC_HAZARD;
                case FailedToCompensate:
                case FailedToComplete:
                    // the participant could not finish - log a warning and forget
                    LRALogger.logger.warnf(
                            "LRAParticipantRecord.doEnd(compensate %b) get status %s did not finish: %s: WILL NOT RETRY",
                            compensate, endPath, reportedStatus);

                    if (forgetEndpoint != null) {
                        if (!forget()) {
                            // we will retry the forget on the next recovery cycle
                            return TwoPhaseOutcome.HEURISTIC_HAZARD;
                        }
                    }

                    return reportFailure(compensate, endPath, "Unknown");
                default:
                    return TwoPhaseOutcome.HEURISTIC_HAZARD;
            }
        }

        return -1;
    }

    private Future<Response> getAsyncResponse(WebTarget target, String method, AsyncInvoker asyncInvoker, String cData) {
        String queryString = target.getUri().getQuery();

        if (queryString != null) {
            String[] queries = queryString.split("&");

            for (String pair : queries) {
                if (pair.contains("=")) {
                    String[] qp = pair.split("=");

                    if (qp[0].equals(LRAConstants.HTTP_METHOD_NAME)) {
                        switch (qp[1]) {
                            case "jakarta.ws.rs.GET":
                                return asyncInvoker.get();
                            case "jakarta.ws.rs.PUT":
                                return asyncInvoker.put(Entity.entity(cData, MediaType.TEXT_PLAIN));
                            case "jakarta.ws.rs.POST":
                                return asyncInvoker.post(Entity.entity(cData, MediaType.TEXT_PLAIN));
                            case "jakarta.ws.rs.DELETE":
                                return asyncInvoker.delete();
                            default:
                                break;
                        }
                    }
                }
            }
        }

        switch (method) {
            case "jakarta.ws.rs.GET":
                return asyncInvoker.get();
            case "jakarta.ws.rs.PUT":
                return asyncInvoker.put(Entity.entity(compensatorData, MediaType.TEXT_PLAIN));
            case "jakarta.ws.rs.POST":
                return asyncInvoker.post(Entity.entity(compensatorData, MediaType.TEXT_PLAIN));
            case "jakarta.ws.rs.DELETE":
                return asyncInvoker.delete();
            default:
                return asyncInvoker.get();
        }
    }

    // see if the participant is an LRA in the same VM as the coordinator
    private URI extractParentLRA(URI endPath) {
        if (lraService != null) {
            String[] segments = endPath.getPath().split("/");
            int pCnt = segments.length;

            if (pCnt > 1) {
                String cId;

                try {
                    cId = URLDecoder.decode(segments[pCnt - 2], StandardCharsets.UTF_8);

                    return lraService.hasTransaction(cId) ? new URI(cId) : null;
                } catch (URISyntaxException ignore) {
                }
            }

            if (LRALogger.logger.isTraceEnabled()) {
                trace_progress("extractParentLRA: not local");
            }
        }

        return null;
    }

    private int tryLocalEndInvocation(URI endPath) {
        URI cId = extractParentLRA(endPath);

        if (cId != null) {
            String[] segments = endPath.getPath().split("/");
            int pCnt = segments.length;

            // this is a call from a parent LRA to end the nested LRA:
            boolean isCompensate = COMPENSATE_REL.equals(segments[pCnt - 1]);
            boolean isComplete = COMPLETE_REL.equals(segments[pCnt - 1]);
            int httpStatus;

            if (!isCompensate && !isComplete) {
                if (LRALogger.logger.isInfoEnabled()) {
                    LRALogger.logger.infof("LRAParticipantRecord.doEnd invalid nested participant url %s" +
                            "(should be compensate or complete)",
                            endPath);
                }

                httpStatus = BAD_REQUEST.getStatusCode();
            } else {
                LRAData inVMStatus = lraService.endLRA(cId, isCompensate, true);

                httpStatus = inVMStatus.getHttpStatus();
            }

            return httpStatus;
        }

        // fall back to using JAX-RS

        return -1;
    }

    boolean forget() {
        if (forgetEndpoint == null) {
            LRALogger.logger.warnf(
                    "LRAParticipantRecord.forget() LRA: %s: cannot forget %s: missing forget URI, status: %s",
                    lraId, recoveryURI, status);
            return true;
        }

        if (LRALogger.logger.isTraceEnabled()) {
            trace_progress("forget");
        }

        ParticipantNotifier notifier = ParticipantNotifierHolder.getForEndpoint(forgetEndpoint);
        String lraIdStr = lraId != null ? lraId.toASCIIString() : null;
        String parentIdStr = parentId != null ? parentId.toASCIIString() : null;
        String recoveryStr = recoveryURI != null ? recoveryURI.toASCIIString() : null;

        boolean result = notifier.notifyForget(lraIdStr, parentIdStr, forgetEndpoint, recoveryStr, compensatorData);

        if (result) {
            forgetEndpoint = null; // succeeded so dispose of the endpoint
        } else {
            LRALogger.logger.infof("LRAParticipantRecord.forget %s failed for LRA %s", forgetEndpoint, lraId);
        }

        return result;
    }

    private boolean isCompelete() {
        return status != null && status == ParticipantStatus.Completed;
    }

    private boolean isCompensated() {
        return status != null && status == ParticipantStatus.Compensated;
    }

    @Override
    public boolean save_state(OutputObjectState os, int t) {
        if (super.save_state(os, t)) {
            try {
                packURI(os, lraId);
                os.packString(compensateEndpoint);
                packURI(os, recoveryURI);
                os.packString(completeEndpoint);
                os.packString(afterEndpoint);
                os.packString(statusEndpoint);
                os.packString(forgetEndpoint);
                packStatus(os);
                os.packString(participantPath);
                os.packString(compensatorData);
            } catch (IOException e) {
                LRALogger.logger.warn(LRALogger.i18nLogger.warn_saveState(e.getMessage()));

                return false;
            } finally {
                if (LRALogger.logger.isTraceEnabled()) {
                    trace_progress("saved");
                }
            }
        }

        return true;
    }

    @Override
    public boolean restore_state(InputObjectState os, int t) {
        if (super.restore_state(os, t)) {
            try {
                lraId = unpackURI(os);
                compensateEndpoint = os.unpackString();
                recoveryURI = unpackURI(os);
                completeEndpoint = os.unpackString();
                afterEndpoint = os.unpackString();
                statusEndpoint = os.unpackString();
                forgetEndpoint = os.unpackString();
                unpackStatus(os);
                participantPath = os.unpackString();
                compensatorData = os.unpackString();
                accepted = status == ParticipantStatus.Completing || status == ParticipantStatus.Compensating;
            } catch (IOException | URISyntaxException e) {
                LRALogger.i18nLogger.warn_restoreState(e.getMessage());
                return false;
            } finally {
                if (LRALogger.logger.isTraceEnabled()) {
                    trace_progress("restored");
                }
            }
        }

        return true;
    }

    private void packStatus(OutputObjectState os) throws IOException {
        if (status == null) {
            os.packBoolean(false);
        } else {
            os.packBoolean(true);
            os.packInt(status.ordinal());
        }
    }

    private void unpackStatus(InputObjectState os) throws IOException {
        status = os.unpackBoolean() ? ParticipantStatus.values()[os.unpackInt()] : null;
    }

    private void packURI(OutputObjectState os, URI url) throws IOException {
        if (url == null) {
            os.packBoolean(false);
        } else {
            os.packBoolean(true);
            os.packString(url.toASCIIString());
        }
    }

    private URI unpackURI(InputObjectState os) throws IOException, URISyntaxException {
        return os.unpackBoolean() ? new URI(Objects.requireNonNull(os.unpackString())) : null;
    }

    private static int getTypeId() {
        return RecordType.LRA_RECORD;
    }

    public int typeIs() {
        return getTypeId();
    }

    public int nestedAbort() {
        return TwoPhaseOutcome.FINISH_OK;
    }

    public int nestedCommit() {
        return TwoPhaseOutcome.FINISH_OK;
    }

    public int nestedPrepare() {
        return TwoPhaseOutcome.PREPARE_OK; // do nothing
    }

    public int nestedOnePhaseCommit() {
        return TwoPhaseOutcome.FINISH_ERROR;
    }

    public String type() {
        return TYPE_NAME;
    }

    public boolean doSave() {
        return true;
    }

    public void merge(AbstractRecord a) {
    }

    public void alter(AbstractRecord a) {
    }

    public boolean shouldAdd(AbstractRecord a) {
        return (a.typeIs() == typeIs());
    }

    public boolean shouldAlter(AbstractRecord a) {
        return false;
    }

    public boolean shouldMerge(AbstractRecord a) {
        return false;
    }

    public boolean shouldReplace(AbstractRecord a) {
        return false;
    }

    @Override
    public Object value() {
        return null;
    }

    @Override
    public void setValue(Object o) {
    }

    @Override
    public int compareTo(AbstractRecord other) {

        if (lessThan(other)) {
            return -1;
        }

        if (greaterThan(other)) {
            return 1;
        }

        return 0;
    }

    public URI getRecoveryURI() {
        return recoveryURI;
    }

    public String getParticipantURI() {
        return participantPath;
    }

    // the participant is asking to be called back on different URLs
    void updateCallbacks(String linkStr) {
        Exception e = parseLink(linkStr);

        if (e != null) {
            throw new WebApplicationException(Response.status(BAD_REQUEST)
                    .entity(LRALogger.i18nLogger.warn_invalid_compensator(e.getMessage(), linkStr))
                    .build());
        }
    }

    void setRecoveryURI(String recoveryURI) {
        try {
            this.recoveryURI = new URI(recoveryURI);
        } catch (URISyntaxException e) {
            String errorMsg = LRALogger.i18nLogger.error_invalidRecoveryUrlToJoinLRAURI(recoveryURI, lraId);

            throw new WebApplicationException(Response.status(BAD_REQUEST)
                    .entity(errorMsg)
                    .build());
        }
    }

    void setRecoveryURI(String recoveryUrlBase, String txId, String participantId) {
        setRecoveryURI(String.format("%s/%s/%s", recoveryUrlBase, txId, participantId));
    }

    public String getCompensator() {
        return compensateEndpoint;
    }

    void setLRAService(LRAService lraService) {
        this.lraService = lraService;
    }

    public void setLraService(LRAService lraService) {
        this.lraService = lraService;
    }

    public String getEndNotificationEndpoint() {
        return afterEndpoint;
    }

    public ParticipantStatus getStatus() {
        return status;
    }

    public void setCompensatorData(String compensatorData) {
        this.previousCompensatorData = this.compensatorData;
        this.compensatorData = compensatorData;
    }

    public String getPreviousCompensatorData() {
        return previousCompensatorData;
    }

    private void trace_progress(String reason) {
        LRALogger.logger.tracef("%s: LRA id: %s, Participant id: %s, reason: %s, state: %s, accepted: %b",
                LocalDateTime.now(ZoneOffset.UTC), // use the same time function as used for LRA timeouts
                lraId,
                participantPath,
                reason,
                status,
                accepted);
    }
}
