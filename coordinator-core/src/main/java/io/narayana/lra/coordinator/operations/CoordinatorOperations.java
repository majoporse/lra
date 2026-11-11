/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.operations;

import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.contracts.LRAInfo;
import io.narayana.lra.coordinator.config.CoordinatorConfig;
import io.narayana.lra.coordinator.domain.model.LongRunningAction;
import io.narayana.lra.coordinator.domain.service.LRAService;
import java.util.List;
import java.util.UUID;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

/**
 * The one coordinator use-case surface behind both the HTTP resources and the Kafka listener.
 * <p>
 * Inputs and outputs are domain values. The adapter decides the HTTP status, the Kafka envelope
 * and the reply topic.
 */
public class CoordinatorOperations {

    private final LRAService lraService;
    private final CoordinatorConfig config;

    public CoordinatorOperations(LRAService lraService, CoordinatorConfig config) {
        this.lraService = lraService;
        this.config = config;
    }

    public StartResult start(UUID parentId, String clientId, long timeout) {
        LongRunningAction lra = lraService.startLRA(config.baseUrl(), parentId, clientId, timeout);

        if (parentId != null) {
            lraService.enlistChildWithParent(parentId, lra.getId(), config.recoveryUrlBase());
        }

        return new StartResult(lra.getId(), parentId);
    }

    public JoinResult join(UUID lraId, long timeLimit, ParticipantCallbacks callbacks,
            String participantId, String userData) {
        StringBuilder recoveryUrl = new StringBuilder();
        StringBuilder data = new StringBuilder(userData == null ? "" : userData);

        lraService.joinLRA(recoveryUrl, lraId, timeLimit, callbacks, config.recoveryUrlBase(), data,
                participantId);

        return new JoinResult(recoveryUrl.toString(), data.toString());
    }

    public LRAInfo close(UUID lraId, String participantId, String userData) {
        return lraService.endLRA(lraId, false, false, participantId, userData);
    }

    public LRAInfo cancel(UUID lraId, String participantId, String userData) {
        return lraService.endLRA(lraId, true, false, participantId, userData);
    }

    public void leave(UUID lraId, String participantId) {
        lraService.leave(lraId, participantId);
    }

    public LRAStatus status(UUID lraId) {
        LRAStatus status = lraService.getTransaction(lraId).getLRAStatus();

        return status == null ? LRAStatus.Active : status;
    }

    public List<LRAInfo> all(LRAStatus filter) {
        return lraService.getAll(filter);
    }

    public LRAInfo info(UUID lraId) {
        return lraService.getLRA(lraId);
    }

    public LRAInfo renew(UUID lraId, long timeLimit) {
        lraService.renewTimeLimit(lraId, timeLimit);

        return lraService.getLRA(lraId);
    }
}
