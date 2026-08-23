/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.domain.model;

import com.arjuna.ats.arjuna.common.Uid;
import com.arjuna.ats.arjuna.coordinator.AbstractRecord;
import com.arjuna.ats.arjuna.coordinator.RecordType;
import com.arjuna.ats.arjuna.coordinator.TwoPhaseOutcome;
import com.arjuna.ats.arjuna.state.InputObjectState;
import com.arjuna.ats.arjuna.state.OutputObjectState;
import io.narayana.lra.LRAData;
import io.narayana.lra.coordinator.domain.LRAException;
import io.narayana.lra.coordinator.domain.model.actions.ActionContext;
import io.narayana.lra.coordinator.domain.model.actions.ActionResult;
import io.narayana.lra.coordinator.domain.model.actions.ActionStatus;
import io.narayana.lra.coordinator.domain.model.actions.HttpAction;
import io.narayana.lra.coordinator.domain.model.actions.LRAAction;
import io.narayana.lra.coordinator.domain.service.LRAService;
import io.narayana.lra.coordinator.domain.service.ParticipantActions;
import io.narayana.lra.logging.LRALogger;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
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

    private LRAAction compensateAction;
    private LRAAction completeAction;
    private LRAAction statusAction;
    private LRAAction forgetAction;
    private LRAAction afterAction;

    private String responseData;
    private String compensatorData;
    private String previousCompensatorData;
    private LRAService lraService;
    private ParticipantStatus status;
    private boolean accepted;
    private LongRunningAction lra;

    public LRAParticipantRecord() {
    }

    LRAParticipantRecord(LongRunningAction lra, LRAService lraService,
            LRAAction compensateAction, LRAAction completeAction,
            LRAAction statusAction, LRAAction forgetAction, LRAAction afterAction,
            String compensatorData) {
        super(new Uid());

        this.lra = lra;
        this.compensateAction = compensateAction;
        this.completeAction = completeAction;
        this.statusAction = statusAction;
        this.forgetAction = forgetAction;
        this.afterAction = afterAction;
        this.lraId = lra.getId();
        this.parentId = lra.getParentId();
        this.status = ParticipantStatus.Active;
        this.lraService = lraService;
        this.participantPath = compensateAction != null ? compensateAction.toJson() : "";
        this.recoveryURI = null;
        this.compensatorData = compensatorData;

        if (LRALogger.logger.isTraceEnabled()) {
            trace_progress("created");
        }
    }

    void setLRA(LongRunningAction lra) {
        this.lra = lra;
        this.parentId = lra.getParentId();
    }

    String getParticipantPath() {
        return participantPath;
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
        LRAAction endAction = compensate ? compensateAction : completeAction;

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

        if (compensateAction == null) {
            return atEnd(TwoPhaseOutcome.FINISH_OK);
        }

        if (compensate) {
            if (isCompensated()) {
                return atEnd(TwoPhaseOutcome.FINISH_OK); // the participant has already compensated
            }

            endAction = compensateAction; // we are going to ask the participant to compensate
            status = ParticipantStatus.Compensating;
        } else {
            if (isCompelete() || completeAction == null) {
                status = ParticipantStatus.Completed;

                return atEnd(TwoPhaseOutcome.FINISH_OK); // the participant has already completed
            }

            endAction = completeAction; // we are going to ask the participant to complete
            status = ParticipantStatus.Completing;
        }

        // NB trying to compensate when already completed is allowed (for nested LRAs)

        ActionStatus actionResultStatus = null;

        if (accepted) {
            // the participant has previously returned a HTTP 202 Accepted response
            // to indicate that it is in progress in which case the status URI
            // must be valid so try that first for the status
            int twoPhaseOutcome = retryGetEndStatus(compensate);

            if (twoPhaseOutcome != -1) {
                return atEnd(twoPhaseOutcome);
            }
        } else {
            int localResult = tryLocalEndInvocation(endAction, compensate); // see if participant is in the same JVM

            if (localResult != -1) {
                // local invocation succeeded
                if (localResult >= 200 && localResult < 300) {
                    actionResultStatus = ActionStatus.OK;
                    responseData = null;
                } else {
                    actionResultStatus = ActionStatus.ERROR;
                }
            }
        }

        if (actionResultStatus == null) {
            // no local invocation — call via action
            if (LRALogger.logger.isTraceEnabled()) {
                trace_progress("notifying participant");
            }

            ActionContext ctx = new ActionContext(
                    lraId.toASCIIString(),
                    parentId != null ? parentId.toASCIIString() : null,
                    recoveryURI != null ? recoveryURI.toASCIIString() : null,
                    compensatorData);

            ActionResult result = endAction.call(ctx);
            actionResultStatus = result.getStatus();
            responseData = result.getBody();

            accepted = actionResultStatus == ActionStatus.ACCEPTED;

            if (accepted && statusAction == null && result.getUpdatedStatusAction() != null) {
                // the participant could not finish immediately and we have no status URI so one should be
                // present in the Location header
                statusAction = result.getUpdatedStatusAction();
            }

            if (actionResultStatus == ActionStatus.GONE) {
                updateStatus(compensate);
                return atEnd(TwoPhaseOutcome.FINISH_OK); // the participant must have finished ok but we lost the response
            }

            if (LRALogger.logger.isTraceEnabled()) {
                trace_progress("notified participant");
            }
        }

        if (responseData != null && actionResultStatus == ActionStatus.ERROR) {
            // the body should contain a valid ParticipantStatus
            try {
                return atEnd(reportFailure(compensate,
                        ParticipantStatus.valueOf(responseData).name()));
            } catch (IllegalArgumentException ignore) {
                // ignore the body and let recovery discover the status of the participant
            }
        }

        if (actionResultStatus == ActionStatus.ERROR && !accepted) {
            if (LRALogger.logger.isDebugEnabled()) {
                LRALogger.logger.debugf("LRAParticipantRecord.doEnd action failed for LRA %s", lraId);
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
        if (compensateAction == null) {
            return afterAction != null;
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

    private boolean afterLRARequest(ActionContext ctx) {
        if (afterAction == null) {
            return true;
        }

        ActionResult result = afterAction.call(ctx);

        if (result.getStatus() == ActionStatus.OK) {
            if (LRALogger.logger.isTraceEnabled()) {
                trace_progress("notified participant");
            }
            return true;
        }

        if (LRALogger.logger.isTraceEnabled()) {
            trace_progress("finished notifying participant");
        }

        return false;
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
            if (afterAction != null) {
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

        ActionContext ctx = new ActionContext(
                lra.getId().toASCIIString(),
                lra.getParentId() != null ? lra.getParentId().toASCIIString() : null,
                recoveryURI != null ? recoveryURI.toASCIIString() : null,
                compensatorData,
                lraStatus.name());

        if (afterAction == null || afterLRARequest(ctx)) {
            afterAction = null;

            if (LRALogger.logger.isTraceEnabled()) {
                trace_progress("runPostLRAActions with afterAction");
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
            return reportFailure(true, failureReason);
        } else { // must be ParticipantStatus.FailedToComplete
            return reportFailure(false, failureReason);
        }
    }

    private int reportFailure(boolean compensate, String failureReason) {
        status = compensate ? ParticipantStatus.FailedToCompensate : ParticipantStatus.FailedToComplete;

        if (LRALogger.logger.isTraceEnabled()) {
            trace_progress("reportFailure");
        }
        LRALogger.logger.warnf("LRAParticipantRecord: participant reported a failure to %s (cause %s)",
                compensate ? COMPENSATE_REL : COMPLETE_REL, failureReason);

        // permanently failed so ask recovery to ignore us in the future.
        return TwoPhaseOutcome.FINISH_ERROR;
    }

    private int retryGetEndStatus(boolean compensate) {
        assert accepted;

        // the participant has previously returned a HTTP 202 Accepted response so the status URI
        // must be valid - try that first for the status

        // first check that this isn't a nested coordinator running locally
        String targetUid = statusAction != null ? statusAction.extractTargetUid() : null;
        URI nestedLraId = null;
        if (targetUid != null && lraService != null && lraService.hasTransaction(targetUid)) {
            try {
                nestedLraId = new URI(targetUid);
            } catch (URISyntaxException ignore) {
            }
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
                            nestedLraId);
                    return -1; // shouldn't happen since it implies it's still active - force end to be called
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
                        return reportFailure(compensate, "unknown");
                    default:
                        return TwoPhaseOutcome.HEURISTIC_HAZARD;
                }
            }
        } else if (statusAction != null) {
            // it is a standard participant - check the status action
            ActionContext ctx = new ActionContext(
                    lraId.toASCIIString(),
                    parentId != null ? parentId.toASCIIString() : null,
                    recoveryURI != null ? recoveryURI.toASCIIString() : null,
                    compensatorData);

            ActionResult result = statusAction.call(ctx);

            if (result.getUpdatedStatusAction() != null) {
                statusAction = result.getUpdatedStatusAction();
            }

            switch (result.getStatus()) {
                case GONE:
                    /*
                     * The specification states (in section 3.2.10. Reporting the status of a participant):
                     * If the participant has already responded successfully to an @Compensate or @Complete method
                     * invocation then it MAY report 410 Gone HTTP status code
                     *
                     * This means that if the participant was asked to compensate then it has now compensated, or
                     * if the participant was asked to complete then it has now completed.
                     */
                    status = compensate ? ParticipantStatus.Compensated : ParticipantStatus.Completed;
                    return TwoPhaseOutcome.FINISH_OK;
                case ACCEPTED:
                case ERROR:
                case TIMEOUT:
                    // these statuses indicate that the implementation should retry later
                    return TwoPhaseOutcome.HEURISTIC_HAZARD;
                case OK:
                    if (result.getBody() == null) {
                        return TwoPhaseOutcome.HEURISTIC_HAZARD;
                    }
                    // the participant is available again and has reported its status
                    try {
                        status = ParticipantStatus.valueOf(result.getBody());
                    } catch (IllegalArgumentException e) {
                        return TwoPhaseOutcome.HEURISTIC_HAZARD;
                    }

                    switch (status) {
                        case Completed:
                        case Compensated:
                            return TwoPhaseOutcome.FINISH_OK;
                        case Completing:
                        case Compensating:
                            // still in progress - make sure recovery keeps retrying it
                            return TwoPhaseOutcome.HEURISTIC_HAZARD;
                        case FailedToCompensate:
                        case FailedToComplete:
                            // the participant could not finish - log a warning and forget
                            LRALogger.logger.warnf(
                                    "LRAParticipantRecord.doEnd(compensate %b) get status reported failure: %s: WILL NOT RETRY",
                                    compensate, status);

                            if (forgetAction != null) {
                                if (!forget()) {
                                    // we will retry the forget on the next recovery cycle
                                    return TwoPhaseOutcome.HEURISTIC_HAZARD;
                                }
                            }

                            return reportFailure(compensate, "Unknown");
                            default:
                                return TwoPhaseOutcome.HEURISTIC_HAZARD;
                        }
                default:
                    return TwoPhaseOutcome.HEURISTIC_HAZARD;
            }
        }

        return -1;
    }

    private int tryLocalEndInvocation(LRAAction action, boolean compensate) {
        String targetUid = action.extractTargetUid();
        if (targetUid == null || lraService == null || !lraService.hasTransaction(targetUid)) {
            return -1;
        }

        try {
            URI cId = new URI(targetUid);
            LRAData inVMStatus = lraService.endLRA(cId, compensate, true);
            return inVMStatus.getHttpStatus();
        } catch (URISyntaxException e) {
            return -1;
        }
    }

    boolean forget() {
        if (forgetAction != null) {
            ActionContext ctx = new ActionContext(
                    lraId.toASCIIString(),
                    parentId != null ? parentId.toASCIIString() : null,
                    recoveryURI != null ? recoveryURI.toASCIIString() : null,
                    compensatorData);

            ActionResult result = forgetAction.call(ctx);

            if (result.getStatus() == ActionStatus.OK) {
                forgetAction = null; // succeeded so dispose of the action
                return true;
            } else {
                if (LRALogger.logger.isInfoEnabled()) {
                    LRALogger.logger.infof("LRAParticipantRecord.forget failed for LRA %s (status: %s)",
                            lraId, result.getStatus());
                }
                return false; // force recovery to keep retrying
            }
        } else {
            LRALogger.logger.warnf(
                    "LRAParticipantRecord.forget() LRA: %s: cannot forget: missing forget action, status: %s",
                    lraId, status);
        }

        return true;
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
                packAction(os, compensateAction);
                packURI(os, recoveryURI);
                packAction(os, completeAction);
                packAction(os, afterAction);
                packAction(os, statusAction);
                packAction(os, forgetAction);
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
                compensateAction = unpackAction(os);
                recoveryURI = unpackURI(os);
                completeAction = unpackAction(os);
                afterAction = unpackAction(os);
                statusAction = unpackAction(os);
                forgetAction = unpackAction(os);
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

    private void packAction(OutputObjectState os, LRAAction action) throws IOException {
        os.packString(action == null ? null : action.toJson());
    }

    private LRAAction unpackAction(InputObjectState os) throws IOException {
        String json = os.unpackString();
        return LRAAction.fromJson(json);
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
    void updateCallbacks(ParticipantActions actions) {
        if (actions.compensateAction != null)
            this.compensateAction = actions.compensateAction;
        if (actions.completeAction != null)
            this.completeAction = actions.completeAction;
        if (actions.statusAction != null)
            this.statusAction = actions.statusAction;
        if (actions.forgetAction != null)
            this.forgetAction = actions.forgetAction;
        if (actions.afterAction != null)
            this.afterAction = actions.afterAction;
    }

    void setRecoveryURI(String recoveryURI) {
        try {
            this.recoveryURI = new URI(recoveryURI);
        } catch (URISyntaxException e) {
            String errorMsg = LRALogger.i18nLogger.error_invalidRecoveryUrlToJoinLRAURI(recoveryURI, lraId);
            throw new LRAException(errorMsg, LRAException.Type.BAD_REQUEST);
        }
    }

    void setRecoveryURI(String recoveryUrlBase, String txId, String participantId) {
        setRecoveryURI(String.format("%s/%s/%s", recoveryUrlBase, txId, participantId));
    }

    public String getCompensator() {
        if (compensateAction instanceof HttpAction) {
            return ((HttpAction) compensateAction).getUri();
        }
        return compensateAction != null ? compensateAction.toJson() : null;
    }

    public boolean hasAfterAction() {
        return afterAction != null;
    }

    void setLRAService(LRAService lraService) {
        this.lraService = lraService;
    }

    public void setLraService(LRAService lraService) {
        this.lraService = lraService;
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
