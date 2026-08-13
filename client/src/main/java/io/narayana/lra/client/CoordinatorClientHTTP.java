/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

import static jakarta.ws.rs.core.Response.Status.ACCEPTED;
import static jakarta.ws.rs.core.Response.Status.CREATED;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;
import static jakarta.ws.rs.core.Response.Status.OK;
import static jakarta.ws.rs.core.Response.Status.PRECONDITION_FAILED;

import io.narayana.lra.LRAConstants;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.rest.client.RestClientBuilder;

/**
 * HTTP implementation of {@link CoordinatorClient}.
 * Wraps the JAX-RS REST client ({@link HttpCoordinatorClient}).
 */
public class CoordinatorClientHTTP implements CoordinatorClient {

    private final HttpCoordinatorClient httpClient;
    private static final long DEFAULT_TIMEOUT_SECONDS = 10;

    public CoordinatorClientHTTP(URI coordinatorUri) {
        RestClientBuilder builder = RestClientBuilder.newBuilder().baseUri(coordinatorUri);
        this.httpClient = new RestClientConfig().configure(builder).build(HttpCoordinatorClient.class);
    }

    public CoordinatorClientHTTP(HttpCoordinatorClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public URI startLRA(String clientId, long timeLimit, URI parentLRA) throws LRAException {
        String encodedParent = parentLRA == null ? "" : parentLRA.toASCIIString();
        try {
            Response response = httpClient.startLRA(
                    clientId, timeLimit, encodedParent,
                    MediaType.TEXT_PLAIN, LRAConstants.CURRENT_API_VERSION_STRING)
                    .toCompletableFuture().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (response.getStatus() == CREATED.getStatusCode()) {
                return URI.create(response.getHeaderString(HttpHeaders.LOCATION));
            }
            throw new LRAException(response.getStatus(),
                    "Unexpected response from startLRA: " + response.getStatus());
        } catch (LRAException e) {
            throw e;
        } catch (Exception e) {
            throw new LRAException("Failed to start LRA", e);
        }
    }

    @Override
    public URI joinLRA(URI lraId, long timeLimit, String registrationData, String compensatorData)
            throws LRAException {
        try {
            Response response = httpClient.joinLRA(
                    lraId.toASCIIString(), timeLimit, registrationData,
                    MediaType.TEXT_PLAIN, LRAConstants.CURRENT_API_VERSION_STRING,
                    compensatorData != null ? compensatorData : "", "")
                    .toCompletableFuture().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (response.getStatus() == OK.getStatusCode()) {
                String recoveryUrl = response.getHeaderString(LRA_HTTP_RECOVERY_HEADER);
                if (recoveryUrl != null) {
                    return URI.create(recoveryUrl);
                }
                throw new LRAException(200, "No recovery URL in joinLRA response");
            }
            throw new LRAException(response.getStatus(),
                    "Unexpected response from joinLRA: " + response.getStatus());
        } catch (LRAException e) {
            throw e;
        } catch (Exception e) {
            throw new LRAException("Failed to join LRA", e);
        }
    }

    @Override
    public void closeLRA(URI lraId, String compensator, String userData) throws LRAException {
        endLRA(lraId, false, compensator, userData);
    }

    @Override
    public void cancelLRA(URI lraId, String compensator, String userData) throws LRAException {
        endLRA(lraId, true, compensator, userData);
    }

    private void endLRA(URI lraId, boolean compensate, String compensator, String userData) throws LRAException {
        try {
            Response response;
            if (compensate) {
                response = httpClient.cancelLRA(
                        lraId.toASCIIString(), MediaType.TEXT_PLAIN,
                        LRAConstants.CURRENT_API_VERSION_STRING, compensator, userData)
                        .toCompletableFuture().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } else {
                response = httpClient.closeLRA(
                        lraId.toASCIIString(), MediaType.TEXT_PLAIN,
                        LRAConstants.CURRENT_API_VERSION_STRING, compensator, userData)
                        .toCompletableFuture().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }

            int status = response.getStatus();
            if (status == OK.getStatusCode() || status == ACCEPTED.getStatusCode()) {
                return;
            }
            if (status == NOT_FOUND.getStatusCode()) {
                throw new LRAException(status, "LRA not found: " + lraId);
            }
            if (status == PRECONDITION_FAILED.getStatusCode()) {
                throw new LRAException(status, "LRA not in active state: " + lraId);
            }
            throw new LRAException(status,
                    "Unexpected response from " + (compensate ? "cancel" : "close") + "LRA: " + status);
        } catch (LRAException e) {
            throw e;
        } catch (Exception e) {
            throw new LRAException("Failed to " + (compensate ? "cancel" : "close") + " LRA", e);
        }
    }

    @Override
    public LRAStatus getLRAStatus(URI lraId) throws LRAException {
        try {
            Response response = httpClient.getLRAStatus(
                    lraId.toASCIIString(), MediaType.TEXT_PLAIN,
                    LRAConstants.CURRENT_API_VERSION_STRING)
                    .toCompletableFuture().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (response.getStatus() == OK.getStatusCode()) {
                String statusStr = response.readEntity(String.class);
                try {
                    return LRAStatus.valueOf(statusStr);
                } catch (Exception e) {
                    throw new LRAException(200, "Invalid LRA status: " + statusStr);
                }
            }
            if (response.getStatus() == NOT_FOUND.getStatusCode()) {
                throw new LRAException(404, "LRA not found: " + lraId);
            }
            throw new LRAException(response.getStatus(),
                    "Unexpected response from getLRAStatus: " + response.getStatus());
        } catch (LRAException e) {
            throw e;
        } catch (Exception e) {
            throw new LRAException("Failed to get LRA status", e);
        }
    }

    @Override
    public void leaveLRA(URI lraId, String compensatorUrl) throws LRAException {
        try {
            Response response = httpClient.leaveLRA(
                    lraId.toASCIIString(), MediaType.TEXT_PLAIN,
                    LRAConstants.CURRENT_API_VERSION_STRING, compensatorUrl)
                    .toCompletableFuture().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (response.getStatus() != OK.getStatusCode()) {
                throw new LRAException(response.getStatus(),
                        "Unexpected response from leaveLRA: " + response.getStatus());
            }
        } catch (LRAException e) {
            throw e;
        } catch (Exception e) {
            throw new LRAException("Failed to leave LRA", e);
        }
    }

    @Override
    public void renewTimeLimit(URI lraId, long timeLimit) throws LRAException {
        try {
            Response response = httpClient.renewTimeLimit(
                    lraId.toASCIIString(), timeLimit,
                    LRAConstants.CURRENT_API_VERSION_STRING)
                    .toCompletableFuture().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (response.getStatus() != OK.getStatusCode()) {
                throw new LRAException(response.getStatus(),
                        "Unexpected response from renewTimeLimit: " + response.getStatus());
            }
        } catch (LRAException e) {
            throw e;
        } catch (Exception e) {
            throw new LRAException("Failed to renew time limit", e);
        }
    }

    @Override
    public LRAStatus getNestedLRAStatus(URI nestedLraId) throws LRAException {
        try {
            Response response = httpClient.getNestedLRAStatus(nestedLraId.toASCIIString())
                    .toCompletableFuture().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (response.getStatus() == OK.getStatusCode()) {
                return LRAStatus.valueOf(response.readEntity(String.class));
            }
            throw new LRAException(response.getStatus(),
                    "Unexpected response from getNestedLRAStatus: " + response.getStatus());
        } catch (LRAException e) {
            throw e;
        } catch (Exception e) {
            throw new LRAException("Failed to get nested LRA status", e);
        }
    }

    @Override
    public void completeNestedLRA(URI nestedLraId) throws LRAException {
        try {
            Response response = httpClient.completeNestedLRA(
                    nestedLraId.toASCIIString(), MediaType.TEXT_PLAIN,
                    LRAConstants.CURRENT_API_VERSION_STRING)
                    .toCompletableFuture().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (response.getStatus() != OK.getStatusCode()) {
                throw new LRAException(response.getStatus(),
                        "Unexpected response from completeNestedLRA: " + response.getStatus());
            }
        } catch (LRAException e) {
            throw e;
        } catch (Exception e) {
            throw new LRAException("Failed to complete nested LRA", e);
        }
    }

    @Override
    public void compensateNestedLRA(URI nestedLraId) throws LRAException {
        try {
            Response response = httpClient.compensateNestedLRA(
                    nestedLraId.toASCIIString(), MediaType.TEXT_PLAIN,
                    LRAConstants.CURRENT_API_VERSION_STRING)
                    .toCompletableFuture().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (response.getStatus() != OK.getStatusCode()) {
                throw new LRAException(response.getStatus(),
                        "Unexpected response from compensateNestedLRA: " + response.getStatus());
            }
        } catch (LRAException e) {
            throw e;
        } catch (Exception e) {
            throw new LRAException("Failed to compensate nested LRA", e);
        }
    }

    @Override
    public void forgetNestedLRA(URI nestedLraId) throws LRAException {
        try {
            Response response = httpClient.forgetNestedLRA(nestedLraId.toASCIIString())
                    .toCompletableFuture().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (response.getStatus() != OK.getStatusCode()) {
                throw new LRAException(response.getStatus(),
                        "Unexpected response from forgetNestedLRA: " + response.getStatus());
            }
        } catch (LRAException e) {
            throw e;
        } catch (Exception e) {
            throw new LRAException("Failed to forget nested LRA", e);
        }
    }

    @Override
    public void close() {
        // REST client doesn't need explicit close
    }

    private static final String LRA_HTTP_RECOVERY_HEADER = "Long-Running-Action-Recovery";
}
