/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.domain.model;

import static io.narayana.lra.LRAConstants.NARAYANA_LRA_PARTICIPANT_DATA_HEADER_NAME;
import static io.narayana.lra.LRAConstants.PARTICIPANT_TIMEOUT;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_ENDED_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_PARENT_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

import io.narayana.lra.coordinator.security.JwtTokenContext;
import io.narayana.lra.logging.LRALogger;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

/**
 * Delivers LRA participant callbacks via synchronous HTTP.
 * This is the default notifier — backward-compatible with existing behavior.
 */
public class HttpParticipantNotifier implements ParticipantNotifier {

    @Override
    public boolean supports(String endpoint) {
        if (endpoint == null) {
            return false;
        }
        return endpoint.startsWith("http://") || endpoint.startsWith("https://");
    }

    @Override
    public int notifyCompensate(String lraId, String parentId, String targetEndpoint,
            String recoveryURI, String compensatorData) {
        return doCallback(lraId, parentId, targetEndpoint, recoveryURI, compensatorData, true);
    }

    @Override
    public int notifyComplete(String lraId, String parentId, String targetEndpoint,
            String recoveryURI, String compensatorData) {
        return doCallback(lraId, parentId, targetEndpoint, recoveryURI, compensatorData, false);
    }

    @Override
    public boolean notifyForget(String lraId, String parentId, String targetEndpoint,
            String recoveryURI, String compensatorData) {
        try (Client client = JwtTokenContext.newClient()) {
            Response response = client.target(URI.create(targetEndpoint))
                    .request()
                    .header(LRA_HTTP_CONTEXT_HEADER, lraId)
                    .header(LRA_HTTP_RECOVERY_HEADER, recoveryURI)
                    .header(LRA_HTTP_PARENT_CONTEXT_HEADER, parentId)
                    .header(NARAYANA_LRA_PARTICIPANT_DATA_HEADER_NAME, compensatorData)
                    .async()
                    .delete()
                    .get(PARTICIPANT_TIMEOUT, TimeUnit.SECONDS);

            return response.getStatus() == Response.Status.OK.getStatusCode();
        } catch (Exception e) {
            LRALogger.logger.infof("HttpParticipantNotifier.forget %s failed for LRA %s (reason %s)",
                    targetEndpoint, lraId, e.getMessage());
            return false;
        }
    }

    @Override
    public boolean notifyAfterLRA(String lraId, String parentId, String targetEndpoint,
            String recoveryURI, String compensatorData, String payload) {
        try (Client client = JwtTokenContext.newClient()) {
            Invocation.Builder builder = client.target(URI.create(targetEndpoint))
                    .request()
                    .header(LRA_HTTP_RECOVERY_HEADER, recoveryURI)
                    .header(NARAYANA_LRA_PARTICIPANT_DATA_HEADER_NAME, compensatorData);

            // For afterLRA, use LRA_HTTP_ENDED_CONTEXT_HEADER
            builder.header(LRA_HTTP_ENDED_CONTEXT_HEADER, lraId);
            if (parentId != null) {
                builder.header(LRA_HTTP_PARENT_CONTEXT_HEADER, parentId);
            }

            Response response = builder.async()
                    .put(Entity.text(payload != null ? payload : ""))
                    .get(PARTICIPANT_TIMEOUT, TimeUnit.SECONDS);

            return response.getStatus() == 200;
        } catch (Exception e) {
            LRALogger.logger.warnf("HttpParticipantNotifier.notifyAfterLRA %s failed for LRA %s: %s",
                    targetEndpoint, lraId, e.getMessage());
            return false;
        }
    }

    @Override
    public ParticipantStatus queryStatus(String lraId, String statusEndpoint,
            String recoveryURI, String compensatorData) {
        try (Client client = JwtTokenContext.newClient()) {
            Response response = client.target(URI.create(statusEndpoint))
                    .request()
                    .header(LRA_HTTP_CONTEXT_HEADER, lraId)
                    .header(LRA_HTTP_RECOVERY_HEADER, recoveryURI)
                    .header(NARAYANA_LRA_PARTICIPANT_DATA_HEADER_NAME, compensatorData)
                    .async()
                    .get()
                    .get(PARTICIPANT_TIMEOUT, TimeUnit.SECONDS);

            if (response.getStatus() == Response.Status.GONE.getStatusCode()) {
                return ParticipantStatus.Compensated; // already finished
            } else if (response.getStatus() == Response.Status.OK.getStatusCode()
                    && response.hasEntity()) {
                return ParticipantStatus.valueOf(response.readEntity(String.class));
            }
            return null; // still in progress or error
        } catch (Exception e) {
            LRALogger.logger.infof("HttpParticipantNotifier.queryStatus %s failed (reason %s)",
                    statusEndpoint, e.getMessage());
            return null;
        }
    }

    private int doCallback(String lraId, String parentId, String targetEndpoint,
            String recoveryURI, String compensatorData, boolean compensate) {
        try (Client client = JwtTokenContext.newClient()) {
            Response response = client.target(URI.create(targetEndpoint))
                    .request()
                    .header(LRA_HTTP_CONTEXT_HEADER, lraId)
                    .header(LRA_HTTP_PARENT_CONTEXT_HEADER, parentId)
                    .header(LRA_HTTP_RECOVERY_HEADER, recoveryURI)
                    .header(NARAYANA_LRA_PARTICIPANT_DATA_HEADER_NAME, compensatorData)
                    .async()
                    .put(Entity.text(""))
                    .get(PARTICIPANT_TIMEOUT, TimeUnit.SECONDS);

            return response.getStatus();
        } catch (Exception e) {
            LRALogger.logger.infof("HttpParticipantNotifier.%s %s failed for LRA %s (reason: %s)",
                    compensate ? "compensate" : "complete", targetEndpoint, lraId, e.getMessage());
            return -1;
        }
    }
}
