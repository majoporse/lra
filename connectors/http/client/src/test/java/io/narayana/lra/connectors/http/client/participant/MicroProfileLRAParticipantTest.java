/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.participant;

import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.contracts.AfterLRACallback;
import io.narayana.lra.connectors.common.contracts.CompensateCallback;
import io.narayana.lra.connectors.common.contracts.ParticipantReply;
import io.narayana.lra.connectors.common.contracts.ParticipantRequest;
import io.narayana.lra.connectors.common.contracts.StatusCallback;
import io.narayana.lra.connectors.http.address.HttpContractAddress;
import io.narayana.lra.connectors.http.address.LegacyHttpAddress;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MicroProfileLRAParticipantTest {

    private static final URI LRA_ID = URI.create("http://localhost:8080/lra/1");
    private static final URI BASE_URI = URI.create("http://service:8080");

    @Test
    public void registersTheJaxRsEndpointsOfTheParticipant() {
        MicroProfileLRAParticipant participant = new MicroProfileLRAParticipant(JaxRsParticipant.class);

        ParticipantCallbacks callbacks = participant.terminationCallbacks(new HttpAddressResolver(),
                BASE_URI.toASCIIString(), BASE_URI);

        LegacyHttpAddress compensate = (LegacyHttpAddress) callbacks.callback(CallbackType.COMPENSATE);
        Assertions.assertEquals(BASE_URI + "/work/compensate", compensate.uri().toASCIIString());
        Assertions.assertEquals("PUT", compensate.method());
        Assertions.assertEquals("ACTIVE", compensate.contextType());

        LegacyHttpAddress status = (LegacyHttpAddress) callbacks.callback(CallbackType.STATUS);
        Assertions.assertEquals(BASE_URI + "/work/status", status.uri().toASCIIString());
        Assertions.assertEquals("GET", status.method());
    }

    @Test
    public void registersGeneratedCallbacksOnTheContractProxy() {
        MicroProfileLRAParticipant participant = new MicroProfileLRAParticipant(PlainParticipant.class);

        ParticipantCallbacks callbacks = participant.terminationCallbacks(new HttpAddressResolver(), "", BASE_URI);

        HttpContractAddress compensate = (HttpContractAddress) callbacks.callback(CallbackType.COMPENSATE);
        Assertions.assertEquals(BASE_URI + "/lra-callback-proxy/" + PlainParticipant.class.getName() + "/compensate",
                compensate.uri().toASCIIString());
    }

    @Test
    public void registersAnEndpointDeclaredOnAnInterface() {
        MicroProfileLRAParticipant participant = new MicroProfileLRAParticipant(InterfaceCallbackParticipant.class);

        ParticipantCallbacks callbacks = participant.terminationCallbacks(new HttpAddressResolver(),
                BASE_URI.toASCIIString(), null);

        LegacyHttpAddress compensate = (LegacyHttpAddress) callbacks.callback(CallbackType.COMPENSATE);
        Assertions.assertEquals(BASE_URI + "/work/compensate", compensate.uri().toASCIIString());
    }

    @Test
    public void mixesEndpointAndGeneratedCallbacks() {
        MicroProfileLRAParticipant participant = new MicroProfileLRAParticipant(MixedParticipant.class);

        ParticipantCallbacks callbacks = participant.terminationCallbacks(new HttpAddressResolver(),
                BASE_URI.toASCIIString(), BASE_URI);

        Assertions.assertInstanceOf(LegacyHttpAddress.class, callbacks.callback(CallbackType.COMPENSATE));
        Assertions.assertInstanceOf(HttpContractAddress.class, callbacks.callback(CallbackType.COMPLETE));
    }

    @Test
    public void mapsASynchronousStatusToAnOutcome() {
        MicroProfileLRAParticipant participant = participant(PlainParticipant.class, new PlainParticipant());

        ParticipantReply reply = participant.invoke(CallbackType.COMPENSATE, request());

        Assertions.assertEquals(ParticipantStatus.Compensated, reply.status);
    }

    @Test
    public void mapsAVoidCallbackToTheDefinitiveOutcome() {
        MicroProfileLRAParticipant participant = participant(VoidParticipant.class, new VoidParticipant());

        Assertions.assertEquals(ParticipantStatus.Completed,
                participant.invoke(CallbackType.COMPLETE, request()).status);
    }

    @Test
    public void mapsAThrownExceptionToAFailedOutcome() {
        MicroProfileLRAParticipant participant = participant(FailingParticipant.class, new FailingParticipant());

        ParticipantReply reply = participant.invoke(CallbackType.COMPENSATE, request());

        Assertions.assertEquals(ParticipantStatus.FailedToCompensate, reply.status);
        Assertions.assertEquals("boom", reply.error);
    }

    @Test
    public void mapsAnAcceptedResponseToAnInProgressOutcome() {
        MicroProfileLRAParticipant participant = participant(AsyncResponseParticipant.class, new AsyncResponseParticipant());

        Assertions.assertEquals(ParticipantStatus.Compensating,
                participant.invoke(CallbackType.COMPENSATE, request()).status);
    }

    @Test
    public void drainsACompletionStageOnStatus() {
        AsyncParticipant instance = new AsyncParticipant();
        MicroProfileLRAParticipant participant = participant(AsyncParticipant.class, instance);

        ParticipantReply first = participant.invoke(CallbackType.COMPENSATE, request());
        Assertions.assertEquals(ParticipantStatus.Compensating, first.status);

        instance.result.complete(ParticipantStatus.FailedToCompensate);

        ParticipantReply status = participant.invoke(CallbackType.STATUS,
                new StatusCallback.Request(plainId(), LRA_ID, null, null, null));

        Assertions.assertEquals(ParticipantStatus.FailedToCompensate, status.status);
    }

    @Test
    public void passesTheLraStatusToAfterLRA() {
        AfterParticipant instance = new AfterParticipant();
        MicroProfileLRAParticipant participant = participant(AfterParticipant.class, instance);

        ParticipantReply reply = participant.invoke(CallbackType.AFTER,
                new AfterLRACallback.Request(plainId(), LRA_ID, null, null, null, LRAStatus.Closed));

        Assertions.assertNull(reply.error);
        Assertions.assertEquals(LRAStatus.Closed, instance.status);
    }

    @Test
    public void rejectsAnInvalidSignature() {
        Assertions.assertThrows(IllegalStateException.class,
                () -> new MicroProfileLRAParticipant(InvalidParticipant.class));
    }

    @Test
    public void returnsNullForAnUndeclaredCallback() {
        MicroProfileLRAParticipant participant = participant(PlainParticipant.class, new PlainParticipant());

        Assertions.assertNull(participant.invoke(CallbackType.FORGET, request()));
    }

    private static MicroProfileLRAParticipant participant(Class<?> javaClass, Object instance) {
        MicroProfileLRAParticipant participant = new MicroProfileLRAParticipant(javaClass);
        participant.setInstance(instance);
        return participant;
    }

    private static ParticipantRequest request() {
        return new CompensateCallback.Request(plainId(), LRA_ID, null, null, null);
    }

    private static String plainId() {
        return PlainParticipant.class.getName();
    }

    // a plain MicroProfile participant: every callback has its own JAX-RS endpoint
    public static class JaxRsParticipant {
        @jakarta.ws.rs.PUT
        @jakarta.ws.rs.Path("/work/compensate")
        @org.eclipse.microprofile.lra.annotation.Compensate
        public ParticipantStatus compensate(URI lraId) {
            return ParticipantStatus.Compensated;
        }

        @jakarta.ws.rs.GET
        @jakarta.ws.rs.Path("/work/status")
        @org.eclipse.microprofile.lra.annotation.Status
        public ParticipantStatus status(URI lraId) {
            return ParticipantStatus.Compensated;
        }
    }

    // a participant with no endpoints of its own: the callbacks are invoked reflectively
    public static class PlainParticipant {
        @org.eclipse.microprofile.lra.annotation.Compensate
        public ParticipantStatus compensate(URI lraId) {
            return ParticipantStatus.Compensated;
        }
    }

    public interface CallbackInterface {

        @jakarta.ws.rs.PUT
        @jakarta.ws.rs.Path("/work/compensate")
        @org.eclipse.microprofile.lra.annotation.Compensate
        ParticipantStatus compensate(URI lraId);
    }

    @jakarta.ws.rs.Path("/callback-interface")
    public static class InterfaceCallbackParticipant implements CallbackInterface {

        @Override
        public ParticipantStatus compensate(URI lraId) {
            return ParticipantStatus.Compensated;
        }
    }

    public static class MixedParticipant {
        @jakarta.ws.rs.PUT
        @jakarta.ws.rs.Path("/work/compensate")
        @org.eclipse.microprofile.lra.annotation.Compensate
        public ParticipantStatus compensate(URI lraId) {
            return ParticipantStatus.Compensated;
        }

        @org.eclipse.microprofile.lra.annotation.Complete
        public ParticipantStatus complete(URI lraId) {
            return ParticipantStatus.Completed;
        }
    }

    public static class VoidParticipant {
        @org.eclipse.microprofile.lra.annotation.Complete
        public void complete(URI lraId, URI parentId) {
        }
    }

    public static class FailingParticipant {
        @org.eclipse.microprofile.lra.annotation.Compensate
        public ParticipantStatus compensate(URI lraId) {
            throw new IllegalStateException("boom");
        }
    }

    public static class AsyncResponseParticipant {
        @org.eclipse.microprofile.lra.annotation.Compensate
        public jakarta.ws.rs.core.Response compensate(URI lraId) {
            return jakarta.ws.rs.core.Response.accepted().build();
        }
    }

    public static class AsyncParticipant {
        final CompletableFuture<ParticipantStatus> result = new CompletableFuture<>();

        @org.eclipse.microprofile.lra.annotation.Compensate
        public CompletionStage<ParticipantStatus> compensate(URI lraId) {
            return result;
        }

        @org.eclipse.microprofile.lra.annotation.Status
        public ParticipantStatus status(URI lraId) {
            return ParticipantStatus.Compensating;
        }
    }

    public static class AfterParticipant {
        LRAStatus status;

        @org.eclipse.microprofile.lra.annotation.AfterLRA
        public void afterLRA(URI lraId, LRAStatus status) {
            this.status = status;
        }
    }

    public static class InvalidParticipant {
        @org.eclipse.microprofile.lra.annotation.Complete
        public ParticipantStatus complete(URI lraId, URI parentId, Object extra) {
            return ParticipantStatus.Completed;
        }
    }
}
