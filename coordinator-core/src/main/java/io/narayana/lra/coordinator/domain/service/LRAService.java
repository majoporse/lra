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
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.contracts.LRAInfo;
import io.narayana.lra.connectors.common.coordinator.CallbackDelivery;
import io.narayana.lra.connectors.common.coordinator.DeliveryRegistry;
import io.narayana.lra.connectors.common.coordinator.ParticipantUriNormaliser;
import io.narayana.lra.coordinator.callback.CallbackSender;
import io.narayana.lra.coordinator.domain.CoordinatorException;
import io.narayana.lra.coordinator.domain.model.LRAParticipantRecord;
import io.narayana.lra.coordinator.domain.model.LongRunningAction;
import io.narayana.lra.coordinator.internal.LRARecoveryModule;
import io.narayana.lra.logging.LRALogger;
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
    private final DeliveryRegistry deliveries = new DeliveryRegistry();
    private final CallbackSender callbackSender = new CallbackSender(deliveries);
    private ParticipantUriNormaliser participantUriNormaliser;
    private LRARecoveryModule recoveryModule;

    public CallbackSender getCallbackSender() {
        return callbackSender;
    }

    /**
     * The coordinator adapter registers the normaliser of the participant wire format it serves.
     * HTTP is always served, so the base coordinator always registers the HTTP normaliser.
     */
    public void registerParticipantUriNormaliser(ParticipantUriNormaliser normaliser) {
        this.participantUriNormaliser = normaliser;
    }

    public String canonicalParticipantUri(String participantUrl) throws URISyntaxException {
        return participantUriNormaliser == null ? participantUrl
                : participantUriNormaliser.canonicalRecoveryUri(participantUrl);
    }

    public String compensatorParticipantUri(String participantUrl) throws URISyntaxException {
        return participantUriNormaliser == null ? participantUrl
                : participantUriNormaliser.compensatorUri(participantUrl);
    }

    public void registerDelivery(CallbackDelivery<?> delivery) {
        // the coordinator adapter may be constructed more than once (tests, redeployments)
        if (!deliveries.addressTypes().contains(delivery.addressType())) {
            deliveries.register(delivery);
        }
    }

    public LongRunningAction getTransaction(UUID lraId) {
        LongRunningAction lra = lras.get(lraId);
        if (lra != null) {
            return lra;
        }

        lra = recoveringLRAs.get(lraId);
        if (lra != null) {
            return lra;
        }

        throw new CoordinatorException(CoordinatorException.Reason.NOT_FOUND,
                "Cannot find transaction id: " + lraId);
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

    public LRAInfo getLRA(UUID lraId) {
        LongRunningAction lra = lookupTransaction(lraId);
        if (lra == null) {
            throw new CoordinatorException(CoordinatorException.Reason.NOT_FOUND,
                    "Cannot find transaction id: " + lraId);
        }
        return toLRAInfo(lra);
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

    public List<LRAInfo> getAll() {
        return getAll(null);
    }

    public List<LRAInfo> getAll(LRAStatus lraStatus) {
        if (lraStatus == null) {
            List<LRAInfo> all = lras.values().stream()
                    .map(this::toLRAInfo).collect(toList());
            all.addAll(getAllRecovering());
            return all;
        }

        List<LRAInfo> allByStatus = getDataByStatus(lras, lraStatus);
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
    public List<LRAInfo> getAllRecovering(boolean scan) {
        if (scan) {
            RecoveryManager.manager().scan();
        }
        return recoveringLRAs.values().stream().map(this::toLRAInfo).collect(toList());
    }

    public List<LRAInfo> getAllRecovering() {
        return getAllRecovering(false);
    }

    private LRAInfo toLRAInfo(LongRunningAction lra) {
        LRAInfo data = LRAInfo.from(lra.getLRAData(), lra.getParticipantStatus());
        data.setLraId(LRAConstants.toLRAId(lra.getId()));
        return data;
    }

    private List<LRAInfo> getDataByStatus(Map<UUID, LongRunningAction> lrasToFilter, LRAStatus status) {
        return lrasToFilter.values().stream().filter(t -> t.getLRAStatus() == status)
                .map(this::toLRAInfo).collect(toList());
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

    public boolean updateRecoveryURI(UUID lraId, ParticipantCallbacks callbacks, String recoveryURI, boolean persist) {
        assert recoveryURI != null;
        assert callbacks != null;
        LongRunningAction transaction = getTransaction(lraId);

        if (transaction.findLRAParticipant(recoveryURI, false) == null) {
            throw new CoordinatorException(CoordinatorException.Reason.NOT_FOUND,
                    String.format("error finding participant with recovery URI: %s in transaction %s",
                            recoveryURI, transaction.getId()));
        }

        if (persist) {
            return transaction.updateRecoveryURI(callbacks, recoveryURI);
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
            throw new CoordinatorException(CoordinatorException.Reason.INVALID_REQUEST,
                    String.format("Invalid base URI: '%s'", baseUri), e);
        }

        status = lra.begin(timelimit);

        if (lra.getLRAStatus() == null) {
            throw new CoordinatorException(CoordinatorException.Reason.UNAVAILABLE,
                    LRALogger.i18nLogger.warn_saveState(LongRunningAction.DEACTIVATE_REASON));
        }

        if (status != ActionStatus.RUNNING) {
            lraTrace(lra.getId().toString(), "failed to start LRA");
            lra.finishLRA(true);

            throw new CoordinatorException(CoordinatorException.Reason.INTERNAL,
                    "Could not start LRA: " + ActionStatus.stringForm(status));
        } else {
            addTransaction(lra);
            return lra;
        }
    }

    public LRAInfo endLRA(UUID lraId, boolean compensate, boolean fromHierarchy) {
        return endLRA(lraId, compensate, fromHierarchy, null, null);
    }

    public LRAInfo endLRA(UUID lraId, boolean compensate, boolean fromHierarchy, String participantId, String userData) {
        LongRunningAction transaction = getTransaction(lraId);

        if (transaction.getLRAStatus() != LRAStatus.Active && !transaction.isRecovering() && transaction.isTopLevel()) {
            throw new CoordinatorException(CoordinatorException.Reason.INVALID_STATE,
                    String.format("%s: LRA is closing or closed: endLRA", lraId));
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

        return toLRAInfo(transaction);
    }

    public void leave(UUID lraId, String participantId) {
        LongRunningAction transaction = getTransaction(lraId);

        if (transaction.getLRAStatus() != LRAStatus.Active) {
            throw new CoordinatorException(CoordinatorException.Reason.INVALID_STATE,
                    "LRA is no longer active: " + lraId);
        }

        boolean wasForgotten;
        try {
            wasForgotten = transaction.forgetParticipant(participantId);
        } catch (Exception e) {
            throw new CoordinatorException(CoordinatorException.Reason.INVALID_REQUEST,
                    String.format("LRAService.forget %s failed on finding participant '%s'", lraId, participantId),
                    e);
        }

        if (!wasForgotten) {
            throw new CoordinatorException(CoordinatorException.Reason.INVALID_REQUEST,
                    String.format("LRAService.forget %s failed as the participant was not found, compensator url '%s'",
                            lraId, participantId));
        }
    }

    public void joinLRA(StringBuilder recoveryUrl, UUID lraId, long timeLimit,
            ParticipantCallbacks actions, String recoveryUrlBase,
            StringBuilder compensatorData, String partId) {
        LongRunningAction transaction = getTransaction(lraId);

        if (timeLimit < 0) {
            timeLimit = 0;
        }

        // the tx must be either Active (for participants with the @Compensate methods) or
        // Closing/Canceling (for the AfterLRA listeners)
        if (transaction.getLRAStatus() != LRAStatus.Active && !transaction.isRecovering()
                && actions.afterCallback == null) {
            throw new CoordinatorException(CoordinatorException.Reason.INVALID_STATE,
                    "LRA is no longer active: " + lraId);
        }

        LRAParticipantRecord participant;

        try {
            if (compensatorData != null) {
                participant = transaction.enlistParticipant(actions, null, recoveryUrlBase,
                        timeLimit, compensatorData.toString(), partId);
                compensatorData.setLength(0);

                if (participant != null && participant.getPreviousCompensatorData() != null) {
                    compensatorData.append(participant.getPreviousCompensatorData());
                }
            } else {
                participant = transaction.enlistParticipant(actions, null, recoveryUrlBase,
                        timeLimit, null, partId);
            }
        } catch (UnsupportedEncodingException e) {
            throw new CoordinatorException(CoordinatorException.Reason.INVALID_REQUEST,
                    "Cannot enlist participant: " + e.getMessage(), e);
        }

        if (participant == null || participant.getRecoveryURI() == null) {
            throw new CoordinatorException(CoordinatorException.Reason.INVALID_STATE,
                    "Participant is already leaving LRA " + lraId);
        }

        String recoveryURI = participant.getRecoveryURI().toASCIIString();

        if (!updateRecoveryURI(lraId, actions, recoveryURI, false)) {
            throw new CoordinatorException(CoordinatorException.Reason.UNAVAILABLE,
                    LRALogger.i18nLogger.warn_saveState(LongRunningAction.DEACTIVATE_REASON));
        }

        recoveryUrl.append(recoveryURI);
    }

    public boolean hasTransaction(UUID id) {
        return id != null && (lras.containsKey(id) || recoveringLRAs.containsKey(id));
    }

    /**
     * Enlist a child LRA as a participant of its parent. Both LRAs belong to this coordinator,
     * so the parent ends the child directly through the participant's {@code childLraId} (the
     * parent and child are never separated by a transport hop).
     *
     * @param parentId the parent LRA id
     * @param childId the child LRA id
     * @param recoveryUrlBase the recovery coordinator URL base used to address this enlistment
     * @return {@code true} if the child was enlisted, {@code false} otherwise (for example the
     *         parent is no longer active)
     */
    public boolean enlistChildWithParent(UUID parentId, UUID childId, String recoveryUrlBase) {
        LongRunningAction parent = lookupTransaction(parentId);
        if (parent == null) {
            return false;
        }

        try {
            parent.enlistParticipant(null, childId, recoveryUrlBase, 0L, null, childId.toString());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void lraTrace(String lraId, String reason) {
        if (LRALogger.logger.isTraceEnabled()) {
            LRALogger.logger.tracef("LRAService: '%s', id: %s%n", reason, lraId);
        }
    }

    public void renewTimeLimit(UUID lraId, Long timelimit) {
        LongRunningAction lra = lras.get(lraId);

        if (lra == null) {
            throw new CoordinatorException(CoordinatorException.Reason.NOT_FOUND,
                    "Cannot find transaction id: " + lraId);
        }

        int status = lra.setTimeLimit(timelimit, true);

        if (status < 200 || status >= 300) {
            throw new CoordinatorException(status == 404 ? CoordinatorException.Reason.NOT_FOUND
                    : status == 412 ? CoordinatorException.Reason.INVALID_STATE
                            : status == 503 ? CoordinatorException.Reason.UNAVAILABLE
                                    : CoordinatorException.Reason.INTERNAL,
                    "Cannot renew LRA " + lraId);
        }
    }

    public List<LRAInfo> getFailedLRAs() {
        Map<UUID, LongRunningAction> failedLRAs = new ConcurrentHashMap<>();
        getRM().getFailedLRAs(failedLRAs);
        return failedLRAs.values().stream().map(this::toLRAInfo).collect(toList());
    }

    private LRARecoveryModule getRM() {
        // since this method is reentrant we do not need any synchronization
        if (recoveryModule == null) {
            recoveryModule = LRARecoveryModule.getInstance();
        }
        return recoveryModule;
    }
}
