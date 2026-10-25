/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.participant;

import io.narayana.lra.api.LRA;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.client.participant.LRAParticipant;
import io.narayana.lra.connectors.common.client.participant.LRAParticipantRegistry;
import io.narayana.lra.connectors.common.contracts.AfterLRACallback;
import io.narayana.lra.connectors.common.contracts.CompensateCallback;
import io.narayana.lra.connectors.common.contracts.CompleteCallback;
import io.narayana.lra.connectors.common.contracts.ForgetCallback;
import io.narayana.lra.connectors.common.contracts.ParticipantReply;
import io.narayana.lra.connectors.common.contracts.StatusCallback;
import io.narayana.lra.connectors.http.address.HttpContractAddress;
import io.narayana.lra.connectors.http.client.outbound.HttpAddressResolver;
import io.narayana.lra.connectors.http.contracts.HttpCompensateCallback;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LRAParticipantTest {

    private static final URI LRA_ID = URI.create("http://localhost:8080/lra/1");

    @Test
    public void acceptsTheStrictCommonSignatures() {
        LRAParticipant participant = new LRAParticipant(SyncParticipant.class);

        Assertions.assertEquals(SyncParticipant.class.getName(), participant.participantId());
        Assertions.assertTrue(participant.hasCallback(CallbackType.COMPENSATE));
        Assertions.assertTrue(participant.hasCallback(CallbackType.COMPLETE));
        Assertions.assertTrue(participant.hasCallback(CallbackType.STATUS));
        Assertions.assertFalse(participant.hasCallback(CallbackType.FORGET));
    }

    @Test
    public void acceptsAnAsynchronousCompensate() {
        LRAParticipant participant = new LRAParticipant(AsyncParticipant.class);

        Assertions.assertTrue(participant.hasCallback(CallbackType.COMPENSATE));
    }

    @Test
    public void rejectsAWrongRequestType() {
        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                () -> new LRAParticipant(WrongRequestParticipant.class));

        Assertions.assertTrue(failure.getMessage().contains("CompensateCallback$Request"));
    }

    @Test
    public void rejectsAWrongReturnType() {
        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                () -> new LRAParticipant(WrongReturnParticipant.class));

        Assertions.assertTrue(failure.getMessage().contains("CompensateCallback$Reply"));
    }

    @Test
    public void listsEveryInvalidCallbackMethod() {
        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                () -> new LRAParticipant(MultipleInvalidParticipant.class));

        Assertions.assertTrue(failure.getMessage().contains("compensate"));
        Assertions.assertTrue(failure.getMessage().contains("complete"));
    }

    @Test
    public void rejectsAnAsynchronousForget() {
        Assertions.assertThrows(IllegalStateException.class,
                () -> new LRAParticipant(AsyncForgetParticipant.class));
    }

    @Test
    public void rejectsAParticipantWithoutATerminationCallback() {
        Assertions.assertThrows(IllegalStateException.class,
                () -> new LRAParticipant(StatusOnlyParticipant.class));
    }

    @Test
    public void rejectsAnAsynchronousTerminationWithoutStatusOrForget() {
        Assertions.assertThrows(IllegalStateException.class,
                () -> new LRAParticipant(AsyncTerminationOnlyParticipant.class));
    }

    @Test
    public void acceptsATransportSpecificRequestSubtype() {
        LRAParticipant participant = new LRAParticipant(SyncParticipant.class);
        participant.setInstance(new SyncParticipant());

        ParticipantReply reply = participant.invoke(CallbackType.COMPENSATE,
                new HttpCompensateCallback.Request(participant.participantId(), LRA_ID, null, null, null));

        Assertions.assertEquals(LRA.ParticipantOutcome.Compensated, reply.status);
    }

    @Test
    public void invokesSynchronousCallbacks() {
        LRAParticipant participant = new LRAParticipant(SyncParticipant.class);
        participant.setInstance(new SyncParticipant());

        ParticipantReply reply = participant.invoke(CallbackType.COMPENSATE, request(participant));

        Assertions.assertInstanceOf(CompensateCallback.Reply.class, reply);
        Assertions.assertEquals(LRA.ParticipantOutcome.Compensated, reply.status);
    }

    @Test
    public void mapsAnExceptionToAFailedOutcome() {
        LRAParticipant participant = new LRAParticipant(FailingParticipant.class);
        participant.setInstance(new FailingParticipant());

        ParticipantReply reply = participant.invoke(CallbackType.COMPENSATE, request(participant));

        Assertions.assertEquals(LRA.ParticipantOutcome.FailedToCompensate, reply.status);
        Assertions.assertTrue(reply.error.contains("boom"));
    }

    @Test
    public void returnsNullForAnUndeclaredCallback() {
        LRAParticipant participant = new LRAParticipant(SyncParticipant.class);
        participant.setInstance(new SyncParticipant());

        Assertions.assertNull(participant.invoke(CallbackType.FORGET, request(participant)));
    }

    @Test
    public void drainsAnAsynchronousResultOnStatus() {
        LRAParticipant participant = new LRAParticipant(AsyncParticipant.class);
        participant.setInstance(new AsyncParticipant());
        CompensateCallback.Request request = request(participant);

        ParticipantReply first = participant.invoke(CallbackType.COMPENSATE, request);
        Assertions.assertEquals(LRA.ParticipantOutcome.Compensating, first.status);

        ParticipantReply status = participant.invoke(CallbackType.STATUS, statusRequest(participant));
        Assertions.assertEquals(LRA.ParticipantOutcome.Compensated, status.status);
    }

    @Test
    public void buildsCallbacksUnderTheCallbackProxyPrefix() {
        LRAParticipant participant = new LRAParticipant(SyncParticipant.class);

        var callbacks = participant.terminationCallbacks(new HttpAddressResolver(), "",
                URI.create("http://service:8080"));
        HttpContractAddress callback = (HttpContractAddress) callbacks.callback(CallbackType.COMPENSATE);

        Assertions.assertEquals("http://service:8080/lra-callback-proxy/" + SyncParticipant.class.getName()
                + "/compensate", callback.uri().toASCIIString());
    }

    @Test
    public void registryDispatchesByParticipantId() {
        LRAParticipant participant = new LRAParticipant(SyncParticipant.class);
        participant.setInstance(new SyncParticipant());
        LRAParticipantRegistry registry = new LRAParticipantRegistry(
                Map.of(participant.participantId(), participant));

        ParticipantReply reply = registry.dispatch(CallbackType.COMPENSATE, request(participant));

        Assertions.assertEquals(LRA.ParticipantOutcome.Compensated, reply.status);
    }

    private static CompensateCallback.Request request(LRAParticipant participant) {
        return new CompensateCallback.Request(participant.participantId(), LRA_ID, null, null, null);
    }

    private static StatusCallback.Request statusRequest(LRAParticipant participant) {
        return new StatusCallback.Request(participant.participantId(), LRA_ID, null, null, null);
    }

    public static class SyncParticipant {
        @LRA.Compensate
        public CompensateCallback.Reply compensate(CompensateCallback.Request request) {
            return new CompensateCallback.Reply(LRA.ParticipantOutcome.Compensated);
        }

        @LRA.Complete
        public CompleteCallback.Reply complete(CompleteCallback.Request request) {
            return new CompleteCallback.Reply(LRA.ParticipantOutcome.Completed);
        }

        @LRA.Status
        public StatusCallback.Reply status(StatusCallback.Request request) {
            return new StatusCallback.Reply(LRA.ParticipantOutcome.Compensated);
        }
    }

    public static class AsyncParticipant {
        @LRA.Compensate
        public CompletionStage<CompensateCallback.Reply> compensate(CompensateCallback.Request request) {
            return CompletableFuture.completedFuture(
                    new CompensateCallback.Reply(LRA.ParticipantOutcome.Compensated));
        }

        @LRA.Status
        public CompletionStage<StatusCallback.Reply> status(StatusCallback.Request request) {
            return CompletableFuture.completedFuture(
                    new StatusCallback.Reply(LRA.ParticipantOutcome.Compensating));
        }
    }

    public static class FailingParticipant {
        @LRA.Compensate
        public CompensateCallback.Reply compensate(CompensateCallback.Request request) {
            throw new IllegalStateException("boom");
        }
    }

    public static class WrongRequestParticipant {
        @LRA.Compensate
        public CompensateCallback.Reply compensate(CompleteCallback.Request request) {
            return new CompensateCallback.Reply(LRA.ParticipantOutcome.Compensated);
        }
    }

    public static class WrongReturnParticipant {
        @LRA.Compensate
        public CompleteCallback.Reply compensate(CompensateCallback.Request request) {
            return new CompleteCallback.Reply(LRA.ParticipantOutcome.Completed);
        }
    }

    public static class MultipleInvalidParticipant {
        @LRA.Compensate
        public CompensateCallback.Reply compensate(CompleteCallback.Request request) {
            return new CompensateCallback.Reply(LRA.ParticipantOutcome.Compensated);
        }

        @LRA.Complete
        public CompensateCallback.Reply complete(CompleteCallback.Request request) {
            return new CompensateCallback.Reply(LRA.ParticipantOutcome.Compensated);
        }
    }

    public static class AsyncForgetParticipant {
        @LRA.Forget
        public CompletionStage<ForgetCallback.Reply> forget(ForgetCallback.Request request) {
            return CompletableFuture.completedFuture(new ForgetCallback.Reply());
        }

        @LRA.AfterLRA
        public AfterLRACallback.Reply after(AfterLRACallback.Request request) {
            return new AfterLRACallback.Reply();
        }
    }

    public static class StatusOnlyParticipant {
        @LRA.Status
        public StatusCallback.Reply status(StatusCallback.Request request) {
            return new StatusCallback.Reply(LRA.ParticipantOutcome.Compensated);
        }
    }

    public static class AsyncTerminationOnlyParticipant {
        @LRA.Compensate
        public CompletionStage<CompensateCallback.Reply> compensate(CompensateCallback.Request request) {
            return CompletableFuture.completedFuture(
                    new CompensateCallback.Reply(LRA.ParticipantOutcome.Compensated));
        }
    }
}
