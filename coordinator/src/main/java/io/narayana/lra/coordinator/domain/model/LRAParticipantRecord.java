/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.domain.model;

import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;

import com.arjuna.ats.arjuna.common.Uid;
import com.arjuna.ats.arjuna.coordinator.AbstractRecord;
import com.arjuna.ats.arjuna.coordinator.RecordType;
import com.arjuna.ats.arjuna.coordinator.TwoPhaseOutcome;
import com.arjuna.ats.arjuna.state.InputObjectState;
import com.arjuna.ats.arjuna.state.OutputObjectState;
import io.narayana.lra.Current;
import io.narayana.lra.LRAData;
import io.narayana.lra.callbacks.CallbackContext;
import io.narayana.lra.callbacks.CallbackResult;
import io.narayana.lra.callbacks.CallbackStatus;
import io.narayana.lra.callbacks.LRACallback;
import io.narayana.lra.callbacks.ParticipantCallbacks;
import io.narayana.lra.coordinator.domain.service.HttpLRAService;
import io.narayana.lra.coordinator.domain.service.LRAService;
import io.narayana.lra.logging.LRALogger;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

public class LRAParticipantRecord extends AbstractRecord implements Comparable<AbstractRecord> {
    private static final String TYPE_NAME = "/StateManager/AbstractRecord/LRARecord";
    private static final String COMPENSATE_REL = "compensate";
    private static final String COMPLETE_REL = "complete";

    private String participantId;
    private UUID lraId;
    private UUID parentId;
    private URI recoveryURI;

    private LRACallback compensateCallback;
    private LRACallback completeCallback;
    private LRACallback statusCallback;
    private LRACallback forgetCallback;
    private LRACallback afterCallback;

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
            LRACallback compensateCallback, LRACallback completeCallback,
            LRACallback statusCallback, LRACallback forgetCallback, LRACallback afterCallback,
            String compensatorData, String partId) {
        super(new Uid());

        this.participantId = partId;
        this.lra = lra;

        this.compensateCallback = compensateCallback;
        this.completeCallback = completeCallback;
        this.statusCallback = statusCallback;
        this.forgetCallback = forgetCallback;
        this.afterCallback = afterCallback;

        this.lraId = lra.getId();
        this.parentId = lra.getParentId();
        this.status = ParticipantStatus.Active;

        this.lraService = lraService;

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
        LRACallback endCallback = compensate ? compensateCallback : completeCallback;

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

        if (compensateCallback == null) {
            return atEnd(TwoPhaseOutcome.FINISH_OK);
        }

        if (compensate) {
            if (isCompensated()) {
                return atEnd(TwoPhaseOutcome.FINISH_OK); // the participant has already compensated
            }

            endCallback = compensateCallback; // we are going to ask the participant to compensate
            status = ParticipantStatus.Compensating;
        } else {
            if (isCompelete() || completeCallback == null) {
                status = ParticipantStatus.Completed;

                return atEnd(TwoPhaseOutcome.FINISH_OK); // the participant has already completed
            }

            endCallback = completeCallback; // we are going to ask the participant to complete
            status = ParticipantStatus.Completing;
        }

        // NB trying to compensate when already completed is allowed (for nested LRAs)

        CallbackStatus callbackResultStatus = null;

        if (accepted) {
            // the participant has previously returned a HTTP 202 Accepted response
            // to indicate that it is in progress in which case the status action
            // must be valid so try that first for the status
            int twoPhaseOutcome = retryGetEndStatus(compensate);

            if (twoPhaseOutcome != -1) {
                return atEnd(twoPhaseOutcome);
            }
        } else {
            var response = tryLocalEndInvocation(endCallback, compensate); // see if participant is in the same JVM
            if (response != null) {
                responseData = response.getBody();
                callbackResultStatus = response.getStatus();
            }

        }

        if (callbackResultStatus == null) {
            // no local invocation — call via callback

            if (LRALogger.logger.isTraceEnabled()) {
                trace_progress("notifying participant");
            }

            CallbackContext ctx = buildCallbackContext(null);

            CallbackResult result = endCallback.call(ctx);
            callbackResultStatus = result.getStatus();
            responseData = result.getBody();

            accepted = callbackResultStatus == CallbackStatus.ACCEPTED;

            if (accepted && statusCallback == null && result.getUpdatedStatusCallback() != null) {
                // the participant could not finish immediately and we have no status action so one should be
                // present in the (updated status) action
                statusCallback = result.getUpdatedStatusCallback();
            }

            if (callbackResultStatus == CallbackStatus.GONE) {
                updateStatus(compensate);
                return atEnd(TwoPhaseOutcome.FINISH_OK); // the participant must have finished ok but we lost the response
            }

            if (LRALogger.logger.isTraceEnabled()) {
                trace_progress("notified participant");
            }
        }

        if (responseData != null && callbackResultStatus == CallbackStatus.FAILED) {
            // the body should contain a valid ParticipantStatus
            try {
                return atEnd(reportFailure(compensate,
                        ParticipantStatus.valueOf(responseData).name()));
            } catch (IllegalArgumentException ignore) {
                // ignore the body and let recovery discover the status of the participant
            }
        }

        if (callbackResultStatus != CallbackStatus.OK && !accepted) {
            if (LRALogger.logger.isDebugEnabled()) {
                LRALogger.logger.debugf("LRAParticipantRecord.doEnd action failed for LRA %s with status: %s",
                        lraId, callbackResultStatus);
            }

            // recovery will figure out the status via the status callback
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
        if (compensateCallback == null) {
            return afterCallback != null;
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

    private boolean afterLRARequest(CallbackContext ctx) {
        if (afterCallback == null) {
            return true;
        }

        CallbackResult result = afterCallback.call(ctx);

        if (result.getStatus() == CallbackStatus.OK) {
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
            if (afterCallback != null) {
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

        CallbackContext ctx = buildCallbackContext(lraStatus.name());

        if (afterCallback == null || afterLRARequest(ctx)) {
            afterCallback = null;

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

        // the participant has previously returned a HTTP 202 Accepted response so the status
        // must be valid - try that first for the status

        // first check that this isn't a nested coordinator running locally
        UUID nestedLraId = null;

        if (statusCallback != null) {
            String targetUid = statusCallback.extractTargetUid();

            if (targetUid != null) {
                try {
                    UUID uuid = UUID.fromString(targetUid);

                    if (lraService != null && lraService.hasTransaction(uuid)) {
                        nestedLraId = uuid;
                    }
                } catch (IllegalArgumentException ignore) {
                }
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
        } else if (statusCallback != null) {
            // it is a standard participant - check the status callback
            try {
                CallbackContext ctx = buildCallbackContext(null);

                CallbackResult result = statusCallback.call(ctx);

                if (result.getUpdatedStatusCallback() != null) {
                    statusCallback = result.getUpdatedStatusCallback();
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
                    case FAILED:
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

                                if (forgetCallback != null) {
                                    if (!forget()) {
                                        // we will retry the forget on the next recovery cycle
                                        return TwoPhaseOutcome.HEURISTIC_HAZARD;
                                    }
                                }

                                return reportFailure(compensate, "Unknown");
                            default:
                                return TwoPhaseOutcome.HEURISTIC_HAZARD;
                        }
                }
            } finally {
                if (LRALogger.logger.isTraceEnabled()) {
                    trace_progress("retryGetEndStatus");
                }
                Current.pop();
            }
        }

        return -1;
    }

    private CallbackContext buildCallbackContext(String payload) {
        return new CallbackContext(
                HttpLRAService.toURI(lra).toASCIIString(),
                parentId == null ? null
                        : HttpLRAService.toURI(lra.getCoordinatorUrl(), parentId, null).toASCIIString(),
                recoveryURI == null ? null : recoveryURI.toASCIIString(),
                compensatorData,
                payload);
    }

    // see if the participant is an LRA in the same VM as the coordinator
    private CallbackResult tryLocalEndInvocation(LRACallback callback, boolean compensate) {
        String targetUid = callback.extractTargetUid();

        if (targetUid == null || lraService == null) {
            return null;
        }

        try {
            UUID cId = UUID.fromString(targetUid);
            if (!lraService.hasTransaction(cId)) {
                return null;
            }

            LRAData inVMStatus = lraService.endLRA(cId, compensate, true);

            return inVMStatus.getCallbackResult();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    boolean forget() {
        if (forgetCallback != null) {
            try {
                CallbackContext ctx = buildCallbackContext(null);

                CallbackResult result = forgetCallback.call(ctx);

                if (result.getStatus() == CallbackStatus.OK) {
                    forgetCallback = null; // succeeded so dispose of the action
                    return true;
                } else {
                    if (LRALogger.logger.isInfoEnabled()) {
                        LRALogger.logger.infof("LRAParticipantRecord.forget failed for LRA %s (status: %s)",
                                lraId, result.getStatus());
                    }
                    return false; // force recovery to keep retrying
                }
            } finally {
                if (LRALogger.logger.isTraceEnabled()) {
                    trace_progress("forget");
                }
                Current.pop();
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
                os.packString(participantId);
                packUUID(os, lraId);
                packUUID(os, parentId);
                packCallback(os, compensateCallback);
                packURI(os, recoveryURI);
                packCallback(os, completeCallback);
                packCallback(os, afterCallback);
                packCallback(os, statusCallback);
                packCallback(os, forgetCallback);
                packStatus(os);
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
                participantId = os.unpackString();
                lraId = unpackUUID(os);
                parentId = unpackUUID(os);
                compensateCallback = unpackCallback(os);
                recoveryURI = unpackURI(os);
                completeCallback = unpackCallback(os);
                afterCallback = unpackCallback(os);
                statusCallback = unpackCallback(os);
                forgetCallback = unpackCallback(os);
                unpackStatus(os);
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

    private void packCallback(OutputObjectState os, LRACallback callback) throws IOException {
        os.packString(callback == null ? null : callback.toJson());
    }

    private LRACallback unpackCallback(InputObjectState os) throws IOException {
        String json = os.unpackString();
        return LRACallback.fromJson(json);
    }

    private void packURI(OutputObjectState os, URI url) throws IOException {
        if (url == null) {
            os.packBoolean(false);
        } else {
            os.packBoolean(true);
            os.packString(url.toASCIIString());
        }
    }

    private void packUUID(OutputObjectState os, UUID uuid) throws IOException {
        if (uuid == null) {
            os.packBoolean(false);
        } else {
            os.packBoolean(true);
            os.packString(uuid.toString());
        }
    }

    private URI unpackURI(InputObjectState os) throws IOException, URISyntaxException {
        return os.unpackBoolean() ? new URI(Objects.requireNonNull(os.unpackString())) : null;
    }

    private UUID unpackUUID(InputObjectState os) throws IOException {
        return os.unpackBoolean() ? UUID.fromString(Objects.requireNonNull(os.unpackString())) : null;
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

    // the participant is asking to be called back on different callbacks
    void updateCallbacks(ParticipantCallbacks callbacks) {
        if (callbacks.compensateCallback != null)
            this.compensateCallback = callbacks.compensateCallback;
        if (callbacks.completeCallback != null)
            this.completeCallback = callbacks.completeCallback;
        if (callbacks.statusCallback != null)
            this.statusCallback = callbacks.statusCallback;
        if (callbacks.forgetCallback != null)
            this.forgetCallback = callbacks.forgetCallback;
        if (callbacks.afterCallback != null)
            this.afterCallback = callbacks.afterCallback;
    }

    void setRecoveryURI(String recoveryURI) {
        try {
            this.recoveryURI = new URI(recoveryURI);
        } catch (URISyntaxException e) {
            String errorMsg = LRALogger.i18nLogger.error_invalidRecoveryUrlToJoinLRAURI(recoveryURI,
                    URI.create("urn:uuid:" + lraId));

            throw new WebApplicationException(Response.status(BAD_REQUEST)
                    .entity(errorMsg)
                    .build());
        }
    }

    void setRecoveryURI(String recoveryUrlBase, String txId, String participantId) {
        setRecoveryURI(String.format("%s/%s/%s", recoveryUrlBase, txId, participantId));
    }

    public LRACallback getCompensateCallback() {
        return compensateCallback;
    }

    public boolean hasAfterCallback() {
        return afterCallback != null;
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
                participantId,
                reason,
                status,
                accepted);
    }

    public String getParticipantId() {
        return participantId;
    }
}
