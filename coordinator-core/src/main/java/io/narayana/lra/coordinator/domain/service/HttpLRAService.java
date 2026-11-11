/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.domain.service;

import io.narayana.lra.LRAConstants;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.contracts.LRAInfo;
import io.narayana.lra.coordinator.domain.CoordinatorException;
import io.narayana.lra.coordinator.domain.model.LongRunningAction;
import io.narayana.lra.logging.LRALogger;
import java.net.URI;
import java.util.UUID;

public class HttpLRAService {
    private static final String COORDINATOR_PATH = "lra-coordinator";

    private final LRAService lraService;
    private String coordinatorUrl;

    public HttpLRAService(LRAService lraService) {
        this.lraService = lraService;
    }

    public String getCoordinatorUrl() {
        if (coordinatorUrl == null) {
            try {
                coordinatorUrl = org.eclipse.microprofile.config.ConfigProvider.getConfig()
                        .getOptionalValue("lra.coordinator.url", String.class)
                        .orElse("http://localhost:8080/" + COORDINATOR_PATH);
            } catch (Exception e) {
                coordinatorUrl = "http://localhost:8080/" + COORDINATOR_PATH;
            }
        }
        return coordinatorUrl;
    }

    // --- UUID extraction ---

    public UUID extractUUID(URI lraId) {
        if (lraId == null) {
            return null;
        }
        String uid = LRAConstants.getLRAUid(lraId);
        if (uid == null || uid.isEmpty()) {
            try {
                return UUID.fromString(lraId.toString());
            } catch (IllegalArgumentException e2) {
                String errorMsg = LRALogger.i18nLogger.warn_invalid_uri(
                        String.valueOf(lraId), "HttpLRAService.extractUUID");
                throw new CoordinatorException(CoordinatorException.Reason.NOT_FOUND, errorMsg);
            }
        }
        try {
            return UUID.fromString(uid);
        } catch (IllegalArgumentException e) {
            String errorMsg = "Cannot extract UUID from LRA id: " + lraId;
            throw new CoordinatorException(CoordinatorException.Reason.NOT_FOUND, errorMsg);
        }
    }

    // --- URI-based delegating methods ---

    public LongRunningAction getTransaction(URI lraId) {
        return lraService.getTransaction(extractUUID(lraId));
    }

    public LongRunningAction lookupTransaction(URI lraId) {
        return lraId == null ? null : lraService.lookupTransaction(extractUUID(lraId));
    }

    public LRAInfo getLRA(URI lraId) {
        return lraService.getLRA(extractUUID(lraId));
    }

    public boolean hasTransaction(URI id) {
        return id != null && lraService.hasTransaction(extractUUID(id));
    }

    public void remove(URI lraId) {
        lraService.remove(extractUUID(lraId));
    }

    public boolean updateRecoveryURI(URI lraId, ParticipantCallbacks actions, String recoveryURI, boolean persist) {
        return lraService.updateRecoveryURI(extractUUID(lraId), actions, recoveryURI, persist);
    }

    public LongRunningAction startLRA(String baseUri, URI parentLRA, String clientId, Long timelimit) {
        UUID parentId = parentLRA == null ? null : extractUUID(parentLRA);
        return lraService.startLRA(baseUri, parentId, clientId, timelimit);
    }

    public LRAInfo endLRA(URI lraId, boolean compensate, boolean fromHierarchy) {
        return lraService.endLRA(extractUUID(lraId), compensate, fromHierarchy);
    }

    public LRAInfo endLRA(URI lraId, boolean compensate, boolean fromHierarchy, String compensator, String userData) {
        return lraService.endLRA(extractUUID(lraId), compensate, fromHierarchy, compensator, userData);
    }

    public void leave(URI lraId, String id) {
        lraService.leave(extractUUID(lraId), id);
    }

    public void joinLRA(StringBuilder recoveryUrl, URI lra, long timeLimit,
            ParticipantCallbacks actions, String recoveryUrlBase,
            StringBuilder compensatorData, String partId) {
        lraService.joinLRA(recoveryUrl, extractUUID(lra), timeLimit,
                actions, recoveryUrlBase, compensatorData, partId);
    }

    public void renewTimeLimit(URI lraId, Long timelimit) {
        lraService.renewTimeLimit(extractUUID(lraId), timelimit);
    }
}
