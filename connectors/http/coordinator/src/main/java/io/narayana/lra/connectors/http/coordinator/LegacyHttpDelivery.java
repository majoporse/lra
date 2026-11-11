/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.coordinator;

import io.narayana.lra.connectors.common.callbacks.CallbackContext;
import io.narayana.lra.connectors.common.callbacks.CallbackResult;
import io.narayana.lra.connectors.common.callbacks.CallbackStatus;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.coordinator.CallbackDelivery;
import io.narayana.lra.connectors.http.address.LegacyHttpAddress;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;

/**
 * Delivers a legacy {@code @Path} callback over HTTP.
 */
@ApplicationScoped
public class LegacyHttpDelivery implements CallbackDelivery<LegacyHttpAddress> {

    @Override
    public Class<LegacyHttpAddress> addressType() {
        return LegacyHttpAddress.class;
    }

    @Override
    public CallbackResult request(LegacyHttpAddress address, CallbackContext context, Duration timeout) {
        try (Client client = ClientBuilder.newClient()) {
            var builder = client.target(address.uri())
                    .request()
                    .header(LRA.LRA_HTTP_RECOVERY_HEADER, context.getRecoveryId())
                    .header("Narayana-LRA-Participant-Data", context.getCompensatorData());

            if (context.getParentId() != null) {
                builder.header(LRA.LRA_HTTP_PARENT_CONTEXT_HEADER, context.getParentId());
            }

            if ("ENDED".equals(address.contextType())) {
                builder.header(LRA.LRA_HTTP_ENDED_CONTEXT_HEADER, context.getLraId());
            } else {
                builder.header(LRA.LRA_HTTP_CONTEXT_HEADER, context.getLraId());
            }

            String payload = context.getPayload() != null ? context.getPayload() : "";

            try (Response response = switch (address.method()) {
                case "GET" -> builder.async().get().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                case "DELETE" -> builder.async().delete().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                default -> builder.async().put(Entity.text(payload)).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            }) {
                CallbackStatus status = mapStatus(response.getStatus());
                String body = response.hasEntity() ? response.readEntity(String.class) : null;
                LegacyHttpAddress updated = status == CallbackStatus.ACCEPTED
                        ? updatedAddress(address, response.getHeaderString("Location"))
                        : null;

                return new CallbackResult(status, body, updated);
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

    static CallbackStatus mapStatus(int status) {
        if (status >= 200 && status < 300) {
            return status == 202 ? CallbackStatus.ACCEPTED : CallbackStatus.OK;
        }

        if (status == 410) {
            return CallbackStatus.GONE;
        }

        if (status == 409) {
            return CallbackStatus.FAILED;
        }

        return CallbackStatus.ERROR;
    }

    static LegacyHttpAddress updatedAddress(LegacyHttpAddress address, String location) {
        return location == null ? null
                : new LegacyHttpAddress(CallbackType.STATUS, address.participantId(), URI.create(location),
                        "PUT", "ACTIVE");
    }
}
