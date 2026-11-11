/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.participant;

import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.client.callback.CallbackReceiver;
import io.narayana.lra.connectors.common.client.participant.LRAParticipant;
import io.narayana.lra.connectors.common.client.participant.LRAParticipantRegistry;
import io.narayana.lra.connectors.common.contracts.AfterLRACallback;
import io.narayana.lra.connectors.common.contracts.CompensateCallback;
import io.narayana.lra.connectors.common.contracts.CompleteCallback;
import io.narayana.lra.connectors.common.contracts.ForgetCallback;
import io.narayana.lra.connectors.common.contracts.ParticipantReply;
import io.narayana.lra.connectors.common.contracts.StatusCallback;
import io.narayana.lra.connectors.http.contracts.HttpAfterLRACallback;
import io.narayana.lra.connectors.http.contracts.HttpCompensateCallback;
import io.narayana.lra.connectors.http.contracts.HttpCompleteCallback;
import io.narayana.lra.connectors.http.contracts.HttpForgetCallback;
import io.narayana.lra.connectors.http.contracts.HttpStatusCallback;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Map;
import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.Forget;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;
import org.eclipse.microprofile.lra.annotation.Status;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LRACallbackResourceTest {

    private static final URI LRA_ID = URI.create("http://localhost:8080/lra/1");

    @Test
    public void sendsADefinitiveOutcomeAsAContract() throws Exception {
        ParticipantReply reply = invoke(CallbackType.COMPENSATE, ParticipantStatus.Compensated, false);

        Assertions.assertEquals(ParticipantStatus.Compensated, reply.status);
    }

    @Test
    public void sendsAnInProgressOutcomeAsAContract() throws Exception {
        ParticipantReply reply = invoke(CallbackType.COMPENSATE, ParticipantStatus.Compensating, false);

        Assertions.assertEquals(ParticipantStatus.Compensating, reply.status);
    }

    @Test
    public void sendsAFailedOutcomeAsAContract() throws Exception {
        ParticipantReply reply = invoke(CallbackType.COMPENSATE, ParticipantStatus.FailedToCompensate, false);

        Assertions.assertEquals(ParticipantStatus.FailedToCompensate, reply.status);
    }

    @Test
    public void sendsAThrownExceptionAsAContractError() throws Exception {
        ParticipantReply reply = invoke(CallbackType.COMPENSATE, ParticipantStatus.Compensated, true);

        Assertions.assertEquals("boom", reply.error);
    }

    @Test
    public void sendsAnUnknownParticipantAsAContractError() throws Exception {
        LRACallbackResource resource = resource(new LRAParticipantRegistry());
        String participantId = "missing";

        ParticipantReply reply = resource.compensate(participantId,
                new HttpCompensateCallback.Request(participantId, LRA_ID, null, null, null));

        Assertions.assertNotNull(reply.error);
    }

    @Test
    public void sendsAMissingRequestAsAContractError() throws Exception {
        ParticipantReply reply = resource(new LRAParticipantRegistry())
                .compensate("missing", null);

        Assertions.assertNotNull(reply.error);
    }

    @Test
    public void acknowledgesAfterLRA() throws Exception {
        LRACallbackResource resource = resourceWith(FlexibleParticipant.class);
        String participantId = FlexibleParticipant.class.getName();

        ParticipantReply reply = resource.afterLRA(participantId,
                new HttpAfterLRACallback.Request(participantId, LRA_ID, null, null, null,
                        LRAStatus.Closed));

        Assertions.assertNull(reply.error);
    }

    private static ParticipantReply invoke(CallbackType type, ParticipantStatus outcome, boolean fail)
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
                            LRAStatus.Closed));
        };
    }

    private static LRACallbackResource resourceWith(Class<?> participantClass) throws Exception {
        LRAParticipant participant = new LRAParticipant(participantClass);
        participant.setInstance(participantClass.getDeclaredConstructor().newInstance());

        return resource(new LRAParticipantRegistry(Map.of(participant.participantId(), participant)));
    }

    private static LRACallbackResource resource(LRAParticipantRegistry registry) throws Exception {
        LRACallbackResource resource = new LRACallbackResource();
        Field field = LRACallbackResource.class.getDeclaredField("callbackReceiver");
        field.setAccessible(true);
        field.set(resource, new CallbackReceiver(registry));

        return resource;
    }

    public static class FlexibleParticipant {
        static ParticipantStatus outcome = ParticipantStatus.Compensated;
        static boolean fail;

        @Compensate
        public CompensateCallback.Reply compensate(CompensateCallback.Request request) {
            failIfAsked();
            return new CompensateCallback.Reply(outcome);
        }

        @Complete
        public CompleteCallback.Reply complete(CompleteCallback.Request request) {
            failIfAsked();
            return new CompleteCallback.Reply(outcome);
        }

        @Status
        public StatusCallback.Reply status(StatusCallback.Request request) {
            failIfAsked();
            return new StatusCallback.Reply(outcome);
        }

        @Forget
        public ForgetCallback.Reply forget(ForgetCallback.Request request) {
            return new ForgetCallback.Reply();
        }

        @AfterLRA
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
