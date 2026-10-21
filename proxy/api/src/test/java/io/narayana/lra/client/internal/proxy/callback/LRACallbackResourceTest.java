/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client.internal.proxy.callback;

import io.narayana.lra.api.LRA;
import io.narayana.lra.callbacks.CallbackReceiver;
import io.narayana.lra.callbacks.CallbackType;
import io.narayana.lra.callbacks.contracts.common.AfterLRACallback;
import io.narayana.lra.callbacks.contracts.common.CompensateCallback;
import io.narayana.lra.callbacks.contracts.common.CompleteCallback;
import io.narayana.lra.callbacks.contracts.common.ForgetCallback;
import io.narayana.lra.callbacks.contracts.common.ParticipantReply;
import io.narayana.lra.callbacks.contracts.common.StatusCallback;
import io.narayana.lra.callbacks.contracts.http.HttpAfterLRACallback;
import io.narayana.lra.callbacks.contracts.http.HttpCompensateCallback;
import io.narayana.lra.callbacks.contracts.http.HttpCompleteCallback;
import io.narayana.lra.callbacks.contracts.http.HttpForgetCallback;
import io.narayana.lra.callbacks.contracts.http.HttpStatusCallback;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LRACallbackResourceTest {

    private static final URI LRA_ID = URI.create("http://localhost:8080/lra/1");

    @Test
    public void sendsADefinitiveOutcomeAsAContract() throws Exception {
        ParticipantReply reply = invoke(CallbackType.COMPENSATE, LRA.ParticipantOutcome.Compensated, false);

        Assertions.assertEquals(LRA.ParticipantOutcome.Compensated, reply.status);
    }

    @Test
    public void sendsAnInProgressOutcomeAsAContract() throws Exception {
        ParticipantReply reply = invoke(CallbackType.COMPENSATE, LRA.ParticipantOutcome.Compensating, false);

        Assertions.assertEquals(LRA.ParticipantOutcome.Compensating, reply.status);
    }

    @Test
    public void sendsAFailedOutcomeAsAContract() throws Exception {
        ParticipantReply reply = invoke(CallbackType.COMPENSATE, LRA.ParticipantOutcome.FailedToCompensate, false);

        Assertions.assertEquals(LRA.ParticipantOutcome.FailedToCompensate, reply.status);
    }

    @Test
    public void sendsAThrownExceptionAsAContractError() throws Exception {
        ParticipantReply reply = invoke(CallbackType.COMPENSATE, LRA.ParticipantOutcome.Compensated, true);

        Assertions.assertEquals("boom", reply.error);
    }

    @Test
    public void sendsAnUnknownParticipantAsAContractError() throws Exception {
        LRACallbackResource resource = resource(new CallbackParticipantRegistry());
        String participantId = "missing";

        ParticipantReply reply = resource.compensate(participantId,
                new HttpCompensateCallback.Request(participantId, LRA_ID, null, null, null));

        Assertions.assertNotNull(reply.error);
    }

    @Test
    public void sendsAMissingRequestAsAContractError() throws Exception {
        ParticipantReply reply = resource(new CallbackParticipantRegistry())
                .compensate("missing", null);

        Assertions.assertNotNull(reply.error);
    }

    @Test
    public void acknowledgesAfterLRA() throws Exception {
        LRACallbackResource resource = resourceWith(FlexibleParticipant.class);
        String participantId = FlexibleParticipant.class.getName();

        ParticipantReply reply = resource.afterLRA(participantId,
                new HttpAfterLRACallback.Request(participantId, LRA_ID, null, null, null,
                        LRA.TransactionStatus.Closed));

        Assertions.assertNull(reply.error);
    }

    private static ParticipantReply invoke(CallbackType type, LRA.ParticipantOutcome outcome, boolean fail)
            throws Exception {
        LRACallbackResource resource = resourceWith(FlexibleParticipant.class);
        FlexibleParticipant.outcome = outcome;
        FlexibleParticipant.fail = fail;
        String participantId = FlexibleParticipant.class.getName();

        return switch (type) {
            case COMPENSATE -> resource.compensate(participantId,
                    new HttpCompensateCallback.Request(participantId, LRA_ID, null, null, null));
            case COMPLETE -> resource.complete(participantId,
                    new HttpCompleteCallback.Request(participantId, LRA_ID, null, null, null));
            case STATUS -> resource.status(participantId,
                    new HttpStatusCallback.Request(participantId, LRA_ID, null, null, null));
            case FORGET -> resource.forget(participantId,
                    new HttpForgetCallback.Request(participantId, LRA_ID, null, null, null));
            case AFTER -> resource.afterLRA(participantId,
                    new HttpAfterLRACallback.Request(participantId, LRA_ID, null, null, null,
                            LRA.TransactionStatus.Closed));
        };
    }

    private static LRACallbackResource resourceWith(Class<?> participantClass) throws Exception {
        ContractParticipant participant = new ContractParticipant(participantClass);
        participant.setInstance(participantClass.getDeclaredConstructor().newInstance());

        return resource(new CallbackParticipantRegistry(Map.of(participant.participantId(), participant)));
    }

    private static LRACallbackResource resource(CallbackParticipantRegistry registry) throws Exception {
        LRACallbackResource resource = new LRACallbackResource();
        Field field = LRACallbackResource.class.getDeclaredField("callbackReceiver");
        field.setAccessible(true);
        field.set(resource, new CallbackReceiver(registry));

        return resource;
    }

    public static class FlexibleParticipant {
        static LRA.ParticipantOutcome outcome = LRA.ParticipantOutcome.Compensated;
        static boolean fail;

        @LRA.Compensate
        public CompensateCallback.Reply compensate(CompensateCallback.Request request) {
            failIfAsked();
            return new CompensateCallback.Reply(outcome);
        }

        @LRA.Complete
        public CompleteCallback.Reply complete(CompleteCallback.Request request) {
            failIfAsked();
            return new CompleteCallback.Reply(outcome);
        }

        @LRA.Status
        public StatusCallback.Reply status(StatusCallback.Request request) {
            failIfAsked();
            return new StatusCallback.Reply(outcome);
        }

        @LRA.Forget
        public ForgetCallback.Reply forget(ForgetCallback.Request request) {
            return new ForgetCallback.Reply();
        }

        @LRA.AfterLRA
        public AfterLRACallback.Reply afterLRA(AfterLRACallback.Request request) {
            return new AfterLRACallback.Reply();
        }

        private static void failIfAsked() {
            if (fail) {
                throw new IllegalStateException("boom");
            }
        }
    }
}
