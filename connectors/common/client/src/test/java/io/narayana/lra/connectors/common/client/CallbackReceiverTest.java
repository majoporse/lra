/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client;

import io.narayana.lra.api.LRA;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.contracts.ParticipantReply;
import io.narayana.lra.connectors.common.contracts.ParticipantRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CallbackReceiverTest {

    private static final String PARTICIPANT_ID = "order-service";

    @Test
    public void rejectsAMissingRequest() {
        CallbackReceiver receiver = new CallbackReceiver((type, request) -> {
            throw new AssertionError("the dispatcher must not be called");
        });

        ParticipantReply reply = receiver.receive(CallbackType.COMPENSATE, null);

        Assertions.assertNotNull(reply.error);
        Assertions.assertNull(reply.status);
    }

    @Test
    public void rejectsAMissingParticipantId() {
        CallbackReceiver receiver = new CallbackReceiver((type, request) -> {
            throw new AssertionError("the dispatcher must not be called");
        });

        ParticipantReply reply = receiver.receive(CallbackType.COMPENSATE, new ParticipantRequest());

        Assertions.assertNotNull(reply.error);
        Assertions.assertNull(reply.status);
    }

    @Test
    public void reportsAMissingDispatcherAsAnError() {
        CallbackReceiver receiver = new CallbackReceiver((ParticipantCallbackDispatcher) null);

        ParticipantReply reply = receiver.receive(CallbackType.COMPLETE, request());

        Assertions.assertNotNull(reply.error);
    }

    @Test
    public void reportsAMissingParticipantAsAnError() {
        CallbackReceiver receiver = new CallbackReceiver((type, request) -> null);

        ParticipantReply reply = receiver.receive(CallbackType.STATUS, request());

        Assertions.assertNotNull(reply.error);
    }

    @Test
    public void reportsAnInvocationFailureAsAnError() {
        CallbackReceiver receiver = new CallbackReceiver((type, request) -> {
            throw new IllegalStateException("boom");
        });

        ParticipantReply reply = receiver.receive(CallbackType.FORGET, request());

        Assertions.assertEquals("boom", reply.error);
    }

    @Test
    public void returnsTheParticipantReplyForEveryCallbackKind() {
        for (CallbackType type : CallbackType.values()) {
            CallbackReceiver receiver = new CallbackReceiver((receivedType, request) -> {
                Assertions.assertEquals(type, receivedType);
                ParticipantReply reply = new ParticipantReply();
                reply.status = LRA.ParticipantOutcome.Compensated;
                return reply;
            });

            ParticipantReply reply = receiver.receive(type, request());

            Assertions.assertEquals(LRA.ParticipantOutcome.Compensated, reply.status);
            Assertions.assertNull(reply.error);
        }
    }

    private static ParticipantRequest request() {
        ParticipantRequest request = new ParticipantRequest();
        request.participantId = PARTICIPANT_ID;
        return request;
    }
}
