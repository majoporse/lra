/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.domain.service;

import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;
import static jakarta.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

import com.arjuna.ats.arjuna.common.Uid;
import com.arjuna.ats.arjuna.coordinator.ActionStatus;
import com.arjuna.ats.arjuna.coordinator.BasicAction;
import com.arjuna.ats.arjuna.recovery.RecoveryManager;
import io.narayana.lra.LRAConstants;
import io.narayana.lra.LRAData;
import io.narayana.lra.contracts.http.ParticipantLinks;
import io.narayana.lra.coordinator.domain.model.LRAParticipantRecord;
import io.narayana.lra.coordinator.domain.model.LongRunningAction;
import io.narayana.lra.coordinator.internal.LRARecoveryModule;
import io.narayana.lra.logging.LRALogger;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

public class LRAService {
    private static final Pattern LINK_REL_PATTERN = Pattern.compile("(\\w+)=\"([^\"]+)\"|([^\\s]+)");

    private final Map<UUID, LongRunningAction> lras = new ConcurrentHashMap<>();
    private final Map<UUID, LongRunningAction> recoveringLRAs = new ConcurrentHashMap<>();
    private final Map<UUID, ReentrantLock> locks = new ConcurrentHashMap<>();
    private LRARecoveryModule recoveryModule;

    public LongRunningAction getTransaction(UUID lraId) throws NotFoundException {
        LongRunningAction lra = lras.get(lraId);
        if (lra != null) {
            return lra;
        }

        lra = recoveringLRAs.get(lraId);
        if (lra != null) {
            return lra;
        }

        String errorMsg = "Cannot find transaction id: " + lraId;
        throw new NotFoundException(errorMsg,
                Response.status(NOT_FOUND).entity(errorMsg).build());
    }

    public LongRunningAction lookupTransaction(UUID lraId) {
        if (lraId == null) {
            return null;
        }
        LongRunningAction lra = lras.get(lraId);
        if (lra != null) {
            return lra;
        }
        return recoveringLRAs.get(lraId);
    }

    public LRAData getLRA(UUID lraId) {
        LongRunningAction lra = lookupTransaction(lraId);
        if (lra == null) {
            throw new NotFoundException("Cannot find transaction id: " + lraId);
        }
        return toLRAData(lra);
    }

    public synchronized ReentrantLock lockTransaction(UUID lraId) {
        ReentrantLock lock = locks.computeIfAbsent(lraId, k -> new ReentrantLock());
        lock.lock();
        return lock;
    }

    public synchronized ReentrantLock tryLockTransaction(UUID lraId) {
        ReentrantLock lock = locks.computeIfAbsent(lraId, k -> new ReentrantLock());
        return lock.tryLock() ? lock : null;
    }

    public synchronized ReentrantLock tryTimedLockTransaction(UUID lraId, long timeout) {
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
                    .map(this::toLRAData).collect(toList());
            all.addAll(getAllRecovering());
            return all;
        }

        List<LRAData> allByStatus = getDataByStatus(lras, lraStatus);
        allByStatus.addAll(getDataByStatus(recoveringLRAs, lraStatus));
        return allByStatus;
    }

    /**
     * Getting all the LRA managed by recovery manager. This means all LRAs which are not mapped
     * only in memory but that were already saved in object store.
     *
     * @param scan defines if there is run recovery manager scanning before returning the collection,
     *        when the recovery is run then the object store is touched and the returned
     *        list may be updated with the new loaded objects
     * @return list of the {@link LRAData} which define the recovering LRAs
     */
    public List<LRAData> getAllRecovering(boolean scan) {
        if (scan) {
            RecoveryManager.manager().scan();
        }
        return recoveringLRAs.values().stream().map(this::toLRAData).collect(toList());
    }

    public List<LRAData> getAllRecovering() {
        return getAllRecovering(false);
    }

    private LRAData toLRAData(LongRunningAction lra) {
        LRAData data = lra.getLRAData();
        data.setLraId(HttpLRAService.toURI(lra));
        return data;
    }

    private List<LRAData> getDataByStatus(Map<UUID, LongRunningAction> lrasToFilter, LRAStatus status) {
        return lrasToFilter.values().stream().filter(t -> t.getLRAStatus() == status)
                .map(this::toLRAData).collect(toList());
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
        String uid = LRAConstants.getLRAUid(lraId);
        try {
            return getRM().removeCommitted(new Uid(uid));
        } catch (Exception e) {
            LRALogger.i18nLogger.warn_cannotRemoveUidRecord(lraId, uid, e);
            return false;
        }
    }

    public void remove(LongRunningAction lra) {
        if (lra.isFailed()) {
            lra.deactivate();
        }
        remove(lra.getId());
    }

    public void remove(UUID uuid) {
        LongRunningAction lra = lras.remove(uuid);
        recoveringLRAs.remove(uuid);
        locks.remove(uuid);
    }

    public void recover() {
        getRM().recover();
    }

    // perform a recovery scan to load any recovering LRAs from the store
    public void scan() {
        getRM().periodicWorkSecondPass(); // periodicWorkFirstPass is a no-op
    }

    public boolean updateRecoveryURI(UUID lraId, ParticipantLinks links, String recoveryURI, boolean persist) {
        assert recoveryURI != null;
        assert links != null;
        LongRunningAction transaction = getTransaction(lraId);

        if (persist) {
            return transaction.updateRecoveryURI(links, recoveryURI);
        }

        return true;
    }

    public LRAParticipantRecord getParticipant(String rcvCoordId) {
        for (var lra : lras.values()) {
            return lra.findLRAParticipant(rcvCoordId, false);
        }
        return null;
    }

    public synchronized LongRunningAction startLRA(String baseUri, UUID parentId, String clientId, Long timelimit) {
        LongRunningAction lra;
        int status;

        try {
            lra = new LongRunningAction(this, baseUri, lookupTransaction(parentId), clientId);
        } catch (URISyntaxException e) {
            throw new WebApplicationException(e.getMessage(),
                    Response.status(Response.Status.PRECONDITION_FAILED)
                            .entity(String.format("Invalid base URI: '%s'", baseUri))
                            .build());
        }

        status = lra.begin(timelimit);

        if (lra.getLRAStatus() == null) {
            throw new WebApplicationException(Response.status(SERVICE_UNAVAILABLE)
                    .entity(LRALogger.i18nLogger.warn_saveState(LongRunningAction.DEACTIVATE_REASON))
                    .build());
        }

        if (status != ActionStatus.RUNNING) {
            lraTrace(lra.getId().toString(), "failed to start LRA");
            lra.finishLRA(true);

            String errorMsg = "Could not start LRA: " + ActionStatus.stringForm(status);

            throw new WebApplicationException(Response.status(INTERNAL_SERVER_ERROR)
                    .entity(errorMsg)
                    .build());
        } else {
            addTransaction(lra);
            return lra;
        }
    }

    public LRAData endLRA(UUID lraId, boolean compensate, boolean fromHierarchy) {
        return endLRA(lraId, compensate, fromHierarchy, null, null);
    }

    public LRAData endLRA(UUID lraId, boolean compensate, boolean fromHierarchy, String participantId, String userData) {
        LongRunningAction transaction = getTransaction(lraId);

        if (transaction.getLRAStatus() != LRAStatus.Active && !transaction.isRecovering() && transaction.isTopLevel()) {
            String errorMsg = String.format("%s: LRA is closing or closed: endLRA", lraId);
            throw new WebApplicationException(errorMsg, Response.status(Response.Status.PRECONDITION_FAILED)
                    .entity(errorMsg).build());
        }

        transaction.finishLRA(compensate, participantId, userData);

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

        return toLRAData(transaction);
    }

    public int leave(UUID lraId, String participantId) {
        LongRunningAction transaction = getTransaction(lraId);

        if (transaction.getLRAStatus() != LRAStatus.Active) {
            return Response.Status.PRECONDITION_FAILED.getStatusCode();
        }

        boolean wasForgotten;
        try {
            wasForgotten = transaction.forgetParticipant(participantId);
        } catch (Exception e) {
            String errorMsg = String.format("LRAService.forget %s failed on finding participant '%s'", lraId, participantId);
            throw new WebApplicationException(errorMsg, e, Response.status(Response.Status.BAD_REQUEST)
                    .entity(errorMsg).build());
        }
        if (wasForgotten) {
            return Response.Status.OK.getStatusCode();
        } else {
            String errorMsg = String.format(
                    "LRAService.forget %s failed as the participant was not found, compensator url '%s'",
                    lraId, participantId);
            throw new WebApplicationException(errorMsg, Response.status(Response.Status.BAD_REQUEST)
                    .entity(errorMsg).build());
        }
    }

    public int joinLRA(StringBuilder recoveryUrl, UUID lraId, long timeLimit,
            ParticipantLinks links, String recoveryUrlBase,
            StringBuilder compensatorData, String partId) {

        LongRunningAction transaction = getTransaction(lraId);

        if (timeLimit < 0) {
            timeLimit = 0;
        }

        // the tx must be either Active (for participants with the @Compensate methods) or
        // Closing/Canceling (for the AfterLRA listeners)

        LRAParticipantRecord participant;

        try {
            if (compensatorData != null) {
                participant = transaction.enlistParticipant(HttpLRAService.toURI(transaction),
                        links, recoveryUrlBase,
                        timeLimit, compensatorData.toString(), partId);
                // return any previously registered data
                compensatorData.setLength(0);

                if (participant != null && participant.getPreviousCompensatorData() != null) {
                    compensatorData.append(participant.getPreviousCompensatorData());
                }
            } else {
                participant = transaction.enlistParticipant(HttpLRAService.toURI(transaction),
                        links, recoveryUrlBase,
                        timeLimit, null, partId);
            }
        } catch (UnsupportedEncodingException e) {
            return Response.Status.PRECONDITION_FAILED.getStatusCode();
        }

        if (participant == null || participant.getRecoveryURI() == null) {
            // probably already closing or cancelling
            return Response.Status.PRECONDITION_FAILED.getStatusCode();
        }

        String recoveryURI = participant.getRecoveryURI().toASCIIString();

        if (!updateRecoveryURI(lraId, links, recoveryURI, false)) {
            String msg = LRALogger.i18nLogger.warn_saveState(LongRunningAction.DEACTIVATE_REASON);
            throw new WebApplicationException(msg, Response.status(SERVICE_UNAVAILABLE)
                    .entity(msg)
                    .build());
        }

        recoveryUrl.append(recoveryURI);

        return Response.Status.OK.getStatusCode();
    }

    public boolean hasTransaction(UUID id) {
        return id != null && (lras.containsKey(id) || recoveringLRAs.containsKey(id));
    }

    private void lraTrace(String lraId, String reason) {
        if (LRALogger.logger.isTraceEnabled()) {
            LRALogger.logger.tracef("LRAService: '%s', id: %s%n", reason, lraId);
        }
    }

    public int renewTimeLimit(UUID lraId, Long timelimit) {
        LongRunningAction lra = lras.get(lraId);

        if (lra == null) {
            return NOT_FOUND.getStatusCode();
        }

        return lra.setTimeLimit(timelimit, true);
    }

    public List<LRAData> getFailedLRAs() {
        Map<UUID, LongRunningAction> failedLRAs = new ConcurrentHashMap<>();
        getRM().getFailedLRAs(failedLRAs);
        return failedLRAs.values().stream().map(this::toLRAData).collect(toList());
    }

    private LRARecoveryModule getRM() {
        // since this method is reentrant we do not need any synchronization
        if (recoveryModule == null) {
            recoveryModule = LRARecoveryModule.getInstance();
        }
        return recoveryModule;
    }
}
