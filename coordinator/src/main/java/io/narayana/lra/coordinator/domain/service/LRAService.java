/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.domain.service;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

import com.arjuna.ats.arjuna.common.Uid;
import com.arjuna.ats.arjuna.coordinator.ActionStatus;
import com.arjuna.ats.arjuna.coordinator.BasicAction;
import com.arjuna.ats.arjuna.recovery.RecoveryManager;
import io.narayana.lra.LRAConstants;
import io.narayana.lra.LRAData;
import io.narayana.lra.coordinator.domain.LRAException;
import io.narayana.lra.coordinator.domain.model.LRAParticipantRecord;
import io.narayana.lra.coordinator.domain.model.LongRunningAction;
import io.narayana.lra.coordinator.internal.LRARecoveryModule;
import io.narayana.lra.logging.LRALogger;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

public class LRAService {
    private static final Pattern LINK_REL_PATTERN = Pattern.compile("(\\w+)=\"([^\"]+)\"|([^\\s]+)");

    private final Map<URI, LongRunningAction> lras = new ConcurrentHashMap<>();
    private final Map<URI, LongRunningAction> recoveringLRAs = new ConcurrentHashMap<>();
    private final Map<URI, ReentrantLock> locks = new ConcurrentHashMap<>();
    private final Map<LongRunningAction, Map<String, String>> lraParticipants = new ConcurrentHashMap<>();
    private LRARecoveryModule recoveryModule;

    public LongRunningAction getTransaction(URI lraId) {
        if (!lras.containsKey(lraId)) {
            String uid = LRAConstants.getLRAUid(lraId);

            if (uid == null || uid.isEmpty()) {
                String errorMsg = LRALogger.i18nLogger.warn_invalid_uri(
                        String.valueOf(lraId), "LongRunningAction.getTransaction");
                throw new LRAException(errorMsg, LRAException.Type.NOT_FOUND);
            }

            // try comparing on uid since different URIs can map to the same resource
            // (eg localhost versus 127.0.0.1 versus :1 etc)
            for (LongRunningAction lra : lras.values()) {
                if (uid.equals(lra.get_uid().fileStringForm())) {
                    return lra;
                }
            }

            if (!recoveringLRAs.containsKey(lraId)) {
                for (LongRunningAction lra : recoveringLRAs.values()) {
                    if (uid.equals(lra.get_uid().fileStringForm())) {
                        return lra;
                    }
                }

                String errorMsg = "Cannot find transaction id: " + lraId;
                throw new LRAException(errorMsg, LRAException.Type.NOT_FOUND);
            }

            return recoveringLRAs.get(lraId);
        }

        return lras.get(lraId);
    }

    public LongRunningAction lookupTransaction(URI lraId) {
        try {
            return lraId == null ? null : getTransaction(lraId);
        } catch (LRAException e) {
            return null;
        }
    }

    public LRAData getLRA(URI lraId) {
        LongRunningAction lra = getTransaction(lraId);
        return lra.getLRAData();
    }

    public synchronized ReentrantLock lockTransaction(URI lraId) {
        ReentrantLock lock = locks.computeIfAbsent(lraId, k -> new ReentrantLock());

        lock.lock();

        return lock;
    }

    public synchronized ReentrantLock tryLockTransaction(URI lraId) {
        ReentrantLock lock = locks.computeIfAbsent(lraId, k -> new ReentrantLock());

        return lock.tryLock() ? lock : null;
    }

    public synchronized ReentrantLock tryTimedLockTransaction(URI lraId, long timeout) {
        ReentrantLock lock = locks.computeIfAbsent(lraId, k -> new ReentrantLock());

        try {
            return lock.tryLock(timeout, MILLISECONDS) ? lock : null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public List<LRAData> getAll() {
        return getAll(null);
    }

    public List<LRAData> getAll(LRAStatus lraStatus) {
        if (lraStatus == null) {
            List<LRAData> all = lras.values().stream()
                    .map(LongRunningAction::getLRAData).collect(toList());
            all.addAll(getAllRecovering());
            return all;
        }

        List<LRAData> allByStatus = getDataByStatus(lras, lraStatus);
        allByStatus.addAll(getDataByStatus(recoveringLRAs, lraStatus));
        return allByStatus;
    }

    public List<LRAData> getAllRecovering(boolean scan) {
        if (scan) {
            RecoveryManager.manager().scan();
        }

        return recoveringLRAs.values().stream().map(LongRunningAction::getLRAData).collect(toList());
    }

    public List<LRAData> getAllRecovering() {
        return getAllRecovering(false);
    }

    public void addTransaction(LongRunningAction lra) {
        lras.putIfAbsent(lra.getId(), lra);
    }

    public void finished(LongRunningAction transaction, boolean fromHierarchy) {
        if (transaction.isFailed()) {
            getRM().moveEntryToFailedLRAPath(transaction.get_uid());
        }
        if (transaction.isRecovering()) {
            recoveringLRAs.put(transaction.getId(), transaction);
        } else if (fromHierarchy || transaction.isTopLevel()) {
            // the LRA is top level or it's a nested LRA that was closed by a
            // parent LRA (ie when fromHierarchy is true) then it's okay to forget about the LRA

            if (!transaction.hasPendingActions()) {
                // this call is only required to clean up cached LRAs (JBTM-3250 will remove this cache).
                remove(transaction);
            }
        }
    }

    /**
     * Remove a log corresponding to an LRA record
     *
     * @param lraId the id of the LRA
     * @return true if the record was either removed or was not present
     */
    public boolean removeLog(String lraId) {
        // LRA ids are URIs with the arjuna uid forming the last segment
        String uid = LRAConstants.getLRAUid(lraId);

        try {
            return getRM().removeCommitted(new Uid(uid));
        } catch (Exception e) {
            LRALogger.i18nLogger.warn_cannotRemoveUidRecord(lraId, uid, e);
            return false;
        }
    }

    public void remove(LongRunningAction lra) {
        if (lra.isFailed()) { // persist failed LRA state
            lra.deactivate();
        }
        remove(lra.getId());
    }

    public void remove(URI lraId) {
        lraTrace(lraId, "remove LRA");

        LongRunningAction lra = lras.remove(lraId);

        if (lra != null) {
            lraParticipants.remove(lra);
        }

        recoveringLRAs.remove(lraId);

        locks.remove(lraId);
    }

    public void recover() {
        getRM().recover();
    }

    // perform a recovery scan to load any recovering LRAs from the store
    public void scan() {
        getRM().periodicWorkSecondPass(); // periodicWorkFirstPass is a no-op
    }

    public boolean updateRecoveryURI(URI lraId, ParticipantActions actions, String recoveryURI, boolean persist) {
        assert recoveryURI != null;
        assert actions != null;
        LongRunningAction transaction = getTransaction(lraId);
        Map<String, String> participants = lraParticipants.get(transaction);
        String compensatorKey = actions.compensateAction != null ? actions.compensateAction.toJson() : "";

        // the <participants> collection should be thread safe against update requests, even though such concurrent
        // updates are improbable because only LRAService.joinLRA and RecoveryCoordinator.replaceCompensator
        // do updates but those are sequential operations anyway
        if (participants == null) {
            participants = new ConcurrentHashMap<>();
            participants.put(recoveryURI, compensatorKey);
            lraParticipants.put(transaction, participants);
        } else {
            participants.replace(recoveryURI, compensatorKey);
        }

        if (persist) {
            return transaction.updateRecoveryURI(actions, recoveryURI);
        }

        return true;
    }

    public String getParticipant(String rcvCoordId) {
        for (Map<String, String> compensators : lraParticipants.values()) {
            String compensator = compensators.get(rcvCoordId);

            if (compensator != null) {
                return compensator;
            }
        }

        return null;
    }

    public synchronized LongRunningAction startLRA(String baseUri, URI parentLRA, String clientId, Long timelimit) {
        LongRunningAction lra;
        int status;

        try {
            lra = new LongRunningAction(this, baseUri, lookupTransaction(parentLRA), clientId);
        } catch (URISyntaxException e) {
            throw new LRAException(String.format("Invalid base URI: '%s'", baseUri),
                    LRAException.Type.PRECONDITION_FAILED);
        }

        status = lra.begin(timelimit);

        if (lra.getLRAStatus() == null) {
            // unable to save state, tell the caller to try again later
            throw new LRAException(LRALogger.i18nLogger.warn_saveState(LongRunningAction.DEACTIVATE_REASON),
                    LRAException.Type.SERVICE_UNAVAILABLE);
        }

        if (status != ActionStatus.RUNNING) {
            lraTrace(lra.getId(), "failed to start LRA");
            lra.finishLRA(true);

            String errorMsg = "Could not start LRA: " + ActionStatus.stringForm(status);
            throw new LRAException(errorMsg, LRAException.Type.INTERNAL_SERVER_ERROR);
        } else {
            addTransaction(lra);

            return lra;
        }
    }

    public LRAData endLRA(URI lraId, boolean compensate, boolean fromHierarchy) {
        return endLRA(lraId, compensate, fromHierarchy, null, null);
    }

    public LRAData endLRA(URI lraId, boolean compensate, boolean fromHierarchy, String compensator, String userData) {
        lraTrace(lraId, "end LRA");

        LongRunningAction transaction = getTransaction(lraId);

        if (transaction.getLRAStatus() != LRAStatus.Active && !transaction.isRecovering() && transaction.isTopLevel()) {
            String errorMsg = String.format("%s: LRA is closing or closed: endLRA", lraId);
            throw new LRAException(errorMsg, LRAException.Type.PRECONDITION_FAILED);
        }

        transaction.finishLRA(compensate, compensator, userData);

        if (BasicAction.Current() != null) {
            if (LRALogger.logger.isInfoEnabled()) {
                LRALogger.logger.infof("LRAServicve.endLRA LRA %s ended but is still associated with %s%n",
                        lraId, BasicAction.Current().get_uid().fileStringForm());
            }
        }

        // Only call finished() if finishLRA actually processed the LRA.
        // If the LRA is still Active it means the lock could not be acquired
        // (another thread is already finishing it) and we must not call finished()
        // because that could prematurely remove an Active LRA from the map.
        if (transaction.getLRAStatus() != LRAStatus.Active) {
            finished(transaction, fromHierarchy);
        }

        return transaction.getLRAData();
    }

    public int leave(URI lraId, String compensatorUrl) {
        lraTrace(lraId, "leave LRA");

        LongRunningAction transaction = getTransaction(lraId);

        if (transaction.getLRAStatus() != LRAStatus.Active) {
            return 412; // PRECONDITION_FAILED
        }

        boolean wasForgotten;
        try {
            wasForgotten = transaction.forgetParticipant(compensatorUrl);
        } catch (Exception e) {
            String errorMsg = String.format("LRAService.forget %s failed on finding participant '%s'", lraId, compensatorUrl);
            throw new LRAException(errorMsg, e, LRAException.Type.BAD_REQUEST);
        }
        if (wasForgotten) {
            return 200; // OK
        } else {
            String errorMsg = String.format(
                    "LRAService.forget %s failed as the participant was not found, compensator url '%s'",
                    lraId, compensatorUrl);
            throw new LRAException(errorMsg, LRAException.Type.BAD_REQUEST);
        }
    }

    public int joinLRA(StringBuilder recoveryUrl, URI lra, long timeLimit,
            ParticipantActions actions, String recoveryUrlBase,
            StringBuilder compensatorData) {
        return joinLRA(recoveryUrl, lra, timeLimit, actions, recoveryUrlBase, compensatorData, null);
    }

    public int joinLRA(StringBuilder recoveryUrl, URI lra, long timeLimit,
            ParticipantActions actions, String recoveryUrlBase,
            StringBuilder compensatorData, String version) {
        if (lra == null) {
            lraTrace(null, "Error missing LRA header in join request");
        } else {
            lraTrace(lra, "join LRA");
        }

        LongRunningAction transaction = getTransaction(lra);

        if (timeLimit < 0) {
            timeLimit = 0;
        }

        // the tx must be either Active (for participants with the @Compensate methods) or
        // Closing/Canceling (for the AfterLRA listeners)
        if (transaction.getLRAStatus() != LRAStatus.Active && !transaction.isRecovering()) {
            if (actions.afterAction == null) {
                return 412; // PRECONDITION_FAILED
            }
        }

        LRAParticipantRecord participant;

        try {
            if (compensatorData != null) {
                participant = transaction.enlistParticipant(lra,
                        actions, recoveryUrlBase,
                        timeLimit, compensatorData.toString(), version);
                // return any previously registered data
                compensatorData.setLength(0);

                if (participant != null && participant.getPreviousCompensatorData() != null) {
                    compensatorData.append(participant.getPreviousCompensatorData());
                }
            } else {
                participant = transaction.enlistParticipant(lra,
                        actions, recoveryUrlBase,
                        timeLimit, null, version);
            }
        } catch (UnsupportedEncodingException e) {
            return 412; // PRECONDITION_FAILED
        }

        if (participant == null || participant.getRecoveryURI() == null) {
            return 412; // PRECONDITION_FAILED
        }

        String recoveryURI = participant.getRecoveryURI().toASCIIString();

        if (!updateRecoveryURI(lra, actions, recoveryURI, false)) {
            String msg = LRALogger.i18nLogger.warn_saveState(LongRunningAction.DEACTIVATE_REASON);
            throw new LRAException(msg, LRAException.Type.SERVICE_UNAVAILABLE);
        }

        recoveryUrl.append(recoveryURI);

        return 200; // OK
    }

    public boolean hasTransaction(URI id) {
        return id != null && (lras.containsKey(id) || recoveringLRAs.containsKey(id));
    }

    public boolean hasTransaction(String id) {
        try {
            return lras.containsKey(new URI(id));
        } catch (URISyntaxException e) {
            return false;
        }
    }

    private void lraTrace(URI lraId, String reason) {
        if (LRALogger.logger.isTraceEnabled()) {
            if (lraId != null && lras.containsKey(lraId)) {
                LongRunningAction lra = lras.get(lraId);
                LRALogger.logger.tracef("LRAService: '%s' (%s) in state %s: %s%n",
                        reason, lra.getClientId(), ActionStatus.stringForm(lra.status()), lra.getId());
            } else {
                LRALogger.logger.tracef("LRAService: '%s', not found: %s%n", reason, lraId);
            }
        }
    }

    public int renewTimeLimit(URI lraId, Long timelimit) {
        LongRunningAction lra = lras.get(lraId);

        if (lra == null) {
            return 404; // NOT_FOUND
        }

        return lra.setTimeLimit(timelimit, true);
    }

    public List<LRAData> getFailedLRAs() {
        Map<URI, LongRunningAction> failedLRAs = new ConcurrentHashMap<>();

        getRM().getFailedLRAs(failedLRAs);

        return failedLRAs.values().stream().map(LongRunningAction::getLRAData).collect(toList());
    }

    private LRARecoveryModule getRM() {
        // since this method is reentrant we do not need any synchronization
        if (recoveryModule == null) {
            recoveryModule = LRARecoveryModule.getInstance();
        }

        return recoveryModule;
    }

    private List<LRAData> getDataByStatus(Map<URI, LongRunningAction> lrasToFilter, LRAStatus status) {
        return lrasToFilter.values().stream().filter(t -> t.getLRAStatus() == status)
                .map(LongRunningAction::getLRAData).collect(toList());
    }
}
