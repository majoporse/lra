/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.lifecycle;

import io.narayana.lra.Current;
import io.narayana.lra.LRAConstants;
import io.narayana.lra.api.Outcome;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.client.coordinator.LRAClient;
import io.narayana.lra.connectors.common.client.coordinator.LRAClientException;
import io.narayana.lra.connectors.common.client.invocation.LraCompletion;
import io.narayana.lra.connectors.common.client.invocation.LraInvocation;
import io.narayana.lra.connectors.common.client.invocation.LraMethodMetadata;
import java.lang.reflect.Method;
import java.net.URI;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ServerLRAProcessorTest {

    private final StubClient client = new StubClient();
    private final ServerLRAProcessor processor = new ServerLRAProcessor(null, null, 0, client, participant -> null);

    @BeforeEach
    public void clearCurrent() {
        Current.popAll();
        Current.clearAuthToken();
    }

    @AfterEach
    public void clearAgain() {
        Current.popAll();
        Current.clearAuthToken();
    }

    @Test
    public void successfulLeaveIsNotReportedAsFailure() throws Exception {
        LraInvocation invocation = leaveOnly();
        invocation.setIncomingLRA(lra());

        processor.processRequest(invocation);

        Assertions.assertFalse(invocation.isAborted());

        LifecycleDecision decision = processor.complete(invocation, LraCompletion.ofStatus(200, null));

        Assertions.assertEquals(1, client.leaves);
        Assertions.assertNull(decision.failureMessage());
        Assertions.assertFalse(decision.hasFailure());
    }

    @Test
    public void failedLeaveAbortsWithTheCoordinatorStatus() throws Exception {
        client.failure = new LRAClientException(412, LRAClientException.Kind.OTHER, "leave failed");
        LraInvocation invocation = leaveOnly();
        invocation.setIncomingLRA(lra());

        processor.processRequest(invocation);

        Assertions.assertTrue(invocation.isAborted());
        Assertions.assertEquals(412, invocation.getAbortStatusCode());

        LifecycleDecision decision = processor.complete(invocation,
                LraCompletion.ofStatus(412, "leave failed"));

        Assertions.assertTrue(decision.hasFailure());
    }

    @Test
    public void invocationWithoutLraNeverCancels() throws Exception {
        LraInvocation invocation = leaveOnly();

        Assertions.assertFalse(processor.isCancel(invocation, Outcome.SERVER_ERROR, true));
        Assertions.assertFalse(processor.isCancel(invocation, 500));
    }

    @Test
    public void invocationWithLraCancelsOnMatchingFamily() throws Exception {
        LraInvocation invocation = invocation(LRA.Type.REQUIRED, Set.of(), Set.of(Outcome.SERVER_ERROR));

        Assertions.assertTrue(processor.isCancel(invocation, Outcome.SERVER_ERROR, false));
        Assertions.assertFalse(processor.isCancel(invocation, Outcome.SUCCESS, false));
        Assertions.assertTrue(processor.isCancel(invocation, 500));
        Assertions.assertFalse(processor.isCancel(invocation, 200));
    }

    private static LraInvocation leaveOnly() throws NoSuchMethodException {
        Method method = Resource.class.getDeclaredMethod("leave");
        LraMethodMetadata metadata = new LraMethodMetadata(null, false, null, null, null, true, false, false);

        return new LraInvocation(method, Resource.class, metadata, List.of(), null);
    }

    private static LraInvocation invocation(LRA.Type type, Set<Integer> cancelOn, Set<Outcome> cancelOnFamily)
            throws NoSuchMethodException {
        Method method = Resource.class.getDeclaredMethod("leave");
        LraMethodMetadata metadata = new LraMethodMetadata(type, false, null, cancelOn, cancelOnFamily, false,
                false, false);

        return new LraInvocation(method, Resource.class, metadata, List.of(), null);
    }

    private static URI lra() {
        return LRAConstants.toLRAId(UUID.randomUUID());
    }

    public static class Resource {
        public void leave() {
        }
    }

    private static final class StubClient implements LRAClient {

        int leaves;
        LRAClientException failure;

        @Override
        public String protocolId() {
            return "stub";
        }

        @Override
        public URI startLRA(URI parentLRA, String clientID, Long timeout, ChronoUnit unit, boolean verbose) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void closeLRA(URI lraId, String compensator, String userData) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void cancelLRA(URI lraId, String compensator, String userData) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void leaveLRA(URI lraId, String body) {
            leaves++;

            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public URI enlistCompensator(URI lraId, Long timeLimit, ParticipantCallbacks callbacks,
                StringBuilder previousParticipantData, String participantId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LRAStatus getStatus(URI lraId) {
            return LRAStatus.Active;
        }

        @Override
        public void close() {
        }
    }
}
