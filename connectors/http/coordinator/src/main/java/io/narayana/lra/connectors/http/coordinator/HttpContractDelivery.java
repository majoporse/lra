/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.api.LRA;
import io.narayana.lra.connectors.common.callbacks.CallbackContext;
import io.narayana.lra.connectors.common.callbacks.CallbackResult;
import io.narayana.lra.connectors.common.callbacks.CallbackStatus;
import io.narayana.lra.connectors.common.contracts.ParticipantReply;
import io.narayana.lra.connectors.common.contracts.ParticipantRequest;
import io.narayana.lra.connectors.common.coordinator.CallbackDelivery;
import io.narayana.lra.connectors.common.coordinator.CallbackOutcomeMapper;
import io.narayana.lra.connectors.http.address.HttpContractAddress;
import io.narayana.lra.connectors.http.contracts.HttpAfterLRACallback;
import io.narayana.lra.connectors.http.contracts.HttpCompensateCallback;
import io.narayana.lra.connectors.http.contracts.HttpCompleteCallback;
import io.narayana.lra.connectors.http.contracts.HttpForgetCallback;
import io.narayana.lra.connectors.http.contracts.HttpStatusCallback;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Delivers the common callback contract over HTTP and maps the contract reply.
 */
@ApplicationScoped
public class HttpContractDelivery implements CallbackDelivery<HttpContractAddress> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Class<HttpContractAddress> addressType() {
        return HttpContractAddress.class;
    }

    @Override
    public CallbackResult request(HttpContractAddress address, CallbackContext context, Duration timeout) {
        try (Client client = ClientBuilder.newClient()) {
            String requestJson = MAPPER.writeValueAsString(newRequest(address, context));

            try (Response response = client.target(address.uri())
                    .request(MediaType.APPLICATION_JSON_TYPE)
                    .async()
                    .post(Entity.entity(requestJson, MediaType.APPLICATION_JSON_TYPE))
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                String body = response.hasEntity() ? response.readEntity(String.class) : null;

                if (body == null || body.isBlank()) {
                    return new CallbackResult(CallbackStatus.ERROR, "Empty contract callback reply");
                }

                ParticipantReply reply;

                try {
                    reply = MAPPER.readValue(body, ParticipantReply.class);
                } catch (Exception e) {
                    return new CallbackResult(CallbackStatus.ERROR, body);
                }

                return CallbackOutcomeMapper.map(address.operation(), reply);
            }
        } catch (TimeoutException e) {
            return new CallbackResult(CallbackStatus.TIMEOUT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new CallbackResult(CallbackStatus.TIMEOUT);
        } catch (Exception e) {
            return new CallbackResult(CallbackStatus.ERROR,
                    e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }
    }

    private static ParticipantRequest newRequest(HttpContractAddress address, CallbackContext context) {
        String lraId = context.getLraId();
        java.net.URI lraUri = lraId == null ? null : java.net.URI.create(lraId);
        java.net.URI parentUri = context.getParentId() == null ? null : java.net.URI.create(context.getParentId());
        String recoveryUrl = context.getRecoveryId();
        String compensatorData = context.getCompensatorData();
        String participantId = address.participantId();

        return switch (address.operation()) {
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
}
