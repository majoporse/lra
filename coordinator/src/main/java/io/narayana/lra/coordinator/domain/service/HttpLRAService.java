/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.domain.service;

import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;

import io.narayana.lra.LRAConstants;
import io.narayana.lra.LRAData;
import io.narayana.lra.contracts.http.ParticipantLinks;
import io.narayana.lra.coordinator.domain.model.LongRunningAction;
import io.narayana.lra.logging.LRALogger;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.core.Response;
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

    // --- URI construction ---

    public URI toURI(UUID lraId) {
        return toURI(lraId, null);
    }

    public URI toURI(UUID lraId, UUID parentId) {
        String url = getCoordinatorUrl();
        int comma = url.indexOf(',');
        if (comma != -1) {
            url = url.substring(0, comma);
        }
        return toURI(url, lraId, parentId);
    }

    public static URI toURI(LongRunningAction lra) {
        String hierarchy = lra.getParentHierarchy();
        if (hierarchy != null) {
            return URI.create(lra.getCoordinatorUrl() + "/" + lra.getId().toString()
                    + "?" + LRAConstants.PARENT_LRA_PARAM_NAME + "=" + hierarchy);
        }
        return URI.create(lra.getCoordinatorUrl() + "/" + lra.getId().toString());
    }

    public static URI toURI(String coordinatorUrl, UUID lraId, UUID parentId) {
        if (parentId != null) {
            return URI.create(
                    coordinatorUrl + "/" + lraId.toString() + "?" + LRAConstants.PARENT_LRA_PARAM_NAME + "="
                            + parentId.toString());
        }
        return URI.create(coordinatorUrl + "/" + lraId.toString());
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
                throw new NotFoundException(errorMsg,
                        Response.status(NOT_FOUND).entity(errorMsg).build());
            }
        }
        try {
            return UUID.fromString(uid);
        } catch (IllegalArgumentException e) {
            String errorMsg = "Cannot extract UUID from LRA id: " + lraId;
            throw new NotFoundException(errorMsg,
                    Response.status(NOT_FOUND).entity(errorMsg).build());
        }
    }

    // --- URI-based delegating methods ---

    public LongRunningAction getTransaction(URI lraId) {
        return lraService.getTransaction(extractUUID(lraId));
    }

    public LongRunningAction lookupTransaction(URI lraId) {
        return lraId == null ? null : lraService.lookupTransaction(extractUUID(lraId));
    }

    public LRAData getLRA(URI lraId) {
        return lraService.getLRA(extractUUID(lraId));
    }

    public boolean hasTransaction(URI id) {
        return id != null && lraService.hasTransaction(extractUUID(id));
    }

    public void remove(URI lraId) {
        lraService.remove(extractUUID(lraId));
    }

    public boolean updateRecoveryURI(URI lraId, ParticipantLinks links, String recoveryURI, boolean persist) {
        return lraService.updateRecoveryURI(extractUUID(lraId), links, recoveryURI, persist);
    }

    public LongRunningAction startLRA(String baseUri, URI parentLRA, String clientId, Long timelimit) {
        UUID parentId = parentLRA == null ? null : extractUUID(parentLRA);
        return lraService.startLRA(baseUri, parentId, clientId, timelimit);
    }

    public LRAData endLRA(URI lraId, boolean compensate, boolean fromHierarchy) {
        return lraService.endLRA(extractUUID(lraId), compensate, fromHierarchy);
    }

    public LRAData endLRA(URI lraId, boolean compensate, boolean fromHierarchy, String compensator, String userData) {
        return lraService.endLRA(extractUUID(lraId), compensate, fromHierarchy, compensator, userData);
    }

    public int leave(URI lraId, String id) {
        return lraService.leave(extractUUID(lraId), id);
    }

    public int joinLRA(StringBuilder recoveryUrl, URI lra, long timeLimit,
            ParticipantLinks links, String recoveryUrlBase,
            StringBuilder compensatorData, String partId) {
        return lraService.joinLRA(recoveryUrl, extractUUID(lra), timeLimit,
                links, recoveryUrlBase, compensatorData, partId);
    }

    public int joinLRA(StringBuilder recoveryUrl, URI lra, long timeLimit,
            ParticipantLinks links, String recoveryUrlBase,
            StringBuilder compensatorData, String version, String partId) {
        return lraService.joinLRA(recoveryUrl, extractUUID(lra), timeLimit,
                links, recoveryUrlBase, compensatorData, version, partId);
    }

    public int renewTimeLimit(URI lraId, Long timelimit) {
        return lraService.renewTimeLimit(extractUUID(lraId), timelimit);
    }
}
