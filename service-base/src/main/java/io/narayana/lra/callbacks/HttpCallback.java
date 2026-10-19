/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.callbacks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.LRAConstants;
import io.narayana.lra.api.LRA;
import io.narayana.lra.callbacks.contracts.common.ParticipantReply;
import io.narayana.lra.callbacks.contracts.common.ParticipantRequest;
import io.narayana.lra.callbacks.contracts.http.HttpAfterLRACallback;
import io.narayana.lra.callbacks.contracts.http.HttpCompensateCallback;
import io.narayana.lra.callbacks.contracts.http.HttpCompleteCallback;
import io.narayana.lra.callbacks.contracts.http.HttpForgetCallback;
import io.narayana.lra.callbacks.contracts.http.HttpStatusCallback;
import io.narayana.lra.logging.LRALogger;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class HttpCallback implements LRACallback {

    public static final String TYPE = "http-contract";

    public static HttpCallback of(CallbackType type, URI uri) {
        return of(type, uri, null);
    }

    public static HttpCallback of(CallbackType type, URI uri, String participantId) {
        return switch (type) {
            case COMPENSATE -> compensateCallback(uri, participantId);
            case COMPLETE -> completeCallback(uri, participantId);
            case STATUS -> statusCallback(uri, participantId);
            case FORGET -> forgetCallback(uri, participantId);
            case AFTER -> afterCallback(uri, participantId);
        };
    }

    public static HttpCallback compensateCallback(URI uri) {
        return compensateCallback(uri, null);
    }

    public static HttpCallback compensateCallback(URI uri, String participantId) {
        return new HttpCallback(uri.toASCIIString(), CallbackType.COMPENSATE, participantId);
    }

    public static HttpCallback completeCallback(URI uri) {
        return completeCallback(uri, null);
    }

    public static HttpCallback completeCallback(URI uri, String participantId) {
        return new HttpCallback(uri.toASCIIString(), CallbackType.COMPLETE, participantId);
    }

    public static HttpCallback statusCallback(URI uri) {
        return statusCallback(uri, null);
    }

    public static HttpCallback statusCallback(URI uri, String participantId) {
        return new HttpCallback(uri.toASCIIString(), CallbackType.STATUS, participantId);
    }

    public static HttpCallback forgetCallback(URI uri) {
        return forgetCallback(uri, null);
    }

    public static HttpCallback forgetCallback(URI uri, String participantId) {
        return new HttpCallback(uri.toASCIIString(), CallbackType.FORGET, participantId);
    }

    public static HttpCallback afterCallback(URI uri) {
        return afterCallback(uri, null);
    }

    public static HttpCallback afterCallback(URI uri, String participantId) {
        return new HttpCallback(uri.toASCIIString(), CallbackType.AFTER, participantId);
    }

    private final String uri;
    private final CallbackType operation;
    private final String participantId;

    @JsonProperty("type")
    public String getType() {
        return TYPE;
    }

    @JsonCreator
    public HttpCallback(
            @JsonProperty("uri") String uri,
            @JsonProperty("operation") CallbackType operation,
            @JsonProperty("participantId") String participantId) {
        this.uri = uri;
        this.operation = operation != null ? operation : CallbackType.COMPLETE;
        this.participantId = participantId;
    }

    public String getUri() {
        return uri;
    }

    public CallbackType getOperation() {
        return operation;
    }

    public String getParticipantId() {
        return participantId;
    }

    public ParticipantRequest newRequest(CallbackContext context) {
        String lraId = context.getLraId();
        URI lraUri = lraId == null ? null : URI.create(lraId);
        URI parentUri = context.getParentId() == null ? null : URI.create(context.getParentId());
        String recoveryUrl = context.getRecoveryId();
        String compensatorData = context.getCompensatorData();

        return switch (operation) {
            case COMPENSATE -> new HttpCompensateCallback.Request(participantId, lraUri, parentUri, recoveryUrl,
                    compensatorData);
            case COMPLETE -> new HttpCompleteCallback.Request(participantId, lraUri, parentUri, recoveryUrl,
                    compensatorData);
            case STATUS -> new HttpStatusCallback.Request(participantId, lraUri, parentUri, recoveryUrl,
                    compensatorData);
            case FORGET -> new HttpForgetCallback.Request(participantId, lraUri, parentUri, recoveryUrl,
                    compensatorData);
            case AFTER -> {
                String payload = context.getPayload();
                yield new HttpAfterLRACallback.Request(participantId, lraUri, parentUri, recoveryUrl,
                        compensatorData, payload == null ? null : LRA.TransactionStatus.valueOf(payload));
            }
        };
    }

    @Override
    public CallbackResult call(CallbackContext context) {
        Client client = null;

        try {
            client = ClientBuilder.newClient();
            String requestJson = new ObjectMapper().writeValueAsString(newRequest(context));
            try (Response response = client.target(uri)
                    .request(MediaType.APPLICATION_JSON_TYPE)
                    .async()
                    .post(Entity.entity(requestJson, MediaType.APPLICATION_JSON_TYPE))
                    .get(LRAConstants.PARTICIPANT_TIMEOUT, TimeUnit.SECONDS)) {
                String body = response.hasEntity() ? response.readEntity(String.class) : null;

                return mapReply(body);
            }
        } catch (TimeoutException e) {
            return new CallbackResult(CallbackStatus.TIMEOUT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new CallbackResult(CallbackStatus.TIMEOUT);
        } catch (Exception e) {
            LRALogger.logger.errorf("HTTP contract callback failed (uri=%s, operation=%s): %s", uri, operation,
                    e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            return new CallbackResult(CallbackStatus.ERROR);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    CallbackResult mapReply(String body) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return switch (operation) {
                case COMPENSATE -> mapEndResult(mapper.readValue(body, HttpCompensateCallback.Reply.class));
                case COMPLETE -> mapEndResult(mapper.readValue(body, HttpCompleteCallback.Reply.class));
                case STATUS -> mapStatusResult(mapper.readValue(body, HttpStatusCallback.Reply.class));
                case FORGET -> mapAckResult(mapper.readValue(body, HttpForgetCallback.Reply.class).error);
                case AFTER -> mapAckResult(mapper.readValue(body, HttpAfterLRACallback.Reply.class).error);
            };
        } catch (Exception e) {
            LRALogger.logger.errorf("Failed to read HTTP contract callback reply (uri=%s, operation=%s): %s", uri,
                    operation, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            return new CallbackResult(CallbackStatus.ERROR);
        }
    }

    private CallbackResult mapEndResult(ParticipantReply reply) {
        if (reply.error != null || reply.status == null) {
            return new CallbackResult(CallbackStatus.ERROR, reply.error);
        }

        return switch (reply.status) {
            case Completed, Compensated -> new CallbackResult(CallbackStatus.OK);
            case Completing, Compensating -> new CallbackResult(CallbackStatus.ACCEPTED);
            case FailedToComplete, FailedToCompensate -> new CallbackResult(CallbackStatus.FAILED,
                    reply.status.name());
            default -> new CallbackResult(CallbackStatus.ERROR);
        };
    }

    private CallbackResult mapStatusResult(ParticipantReply reply) {
        if (reply.error != null || reply.status == null) {
            return new CallbackResult(CallbackStatus.ERROR, reply.error);
        }

        return switch (reply.status) {
            case Completed, Compensated, FailedToComplete, FailedToCompensate ->
                new CallbackResult(CallbackStatus.OK, reply.status.name());
            // in progress: recovery keeps polling
            default -> new CallbackResult(CallbackStatus.ACCEPTED);
        };
    }

    private CallbackResult mapAckResult(String error) {
        return error == null ? new CallbackResult(CallbackStatus.OK) : new CallbackResult(CallbackStatus.ERROR, error);
    }

    @Override
    public String toJson() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize HttpCallback", e);
        }
    }
}
