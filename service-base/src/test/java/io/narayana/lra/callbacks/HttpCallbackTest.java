/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.callbacks;

import io.narayana.lra.api.LRA;
import io.narayana.lra.callbacks.contracts.http.HttpAfterLRACallback;
import io.narayana.lra.callbacks.contracts.http.HttpCompensateCallback;
import io.narayana.lra.callbacks.contracts.http.HttpCompleteCallback;
import io.narayana.lra.callbacks.contracts.http.HttpForgetCallback;
import io.narayana.lra.callbacks.contracts.http.HttpStatusCallback;
import java.net.URI;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HttpCallbackTest {

    private static final URI CALLBACK_URI = URI.create(
            "http://service:8080/lra-callback-proxy/com.example.Participant/compensate");
    private static final CallbackContext CONTEXT = new CallbackContext(
            "http://localhost:8080/lra/1", "http://localhost:8080/lra/parent",
            "http://localhost:8080/recovery/1", "participant-data");

    @Test
    public void buildsTheOperationSpecificContractRequest() {
        HttpCallback callback = HttpCallback.compensateCallback(CALLBACK_URI, "com.example.Participant");

        HttpCompensateCallback.Request request = (HttpCompensateCallback.Request) callback.newRequest(CONTEXT);

        Assertions.assertEquals("com.example.Participant", request.participantId);
        Assertions.assertEquals(URI.create(CONTEXT.getLraId()), request.lraId);
        Assertions.assertEquals(URI.create(CONTEXT.getParentId()), request.parentId);
        Assertions.assertEquals(CONTEXT.getRecoveryId(), request.recoveryUrl);
        Assertions.assertEquals(CONTEXT.getCompensatorData(), request.compensatorData);
    }

    @Test
    public void buildsEachCallbackKind() {
        Assertions.assertInstanceOf(HttpCompensateCallback.Request.class,
                HttpCallback.of(CallbackType.COMPENSATE, CALLBACK_URI, "id").newRequest(CONTEXT));
        Assertions.assertInstanceOf(HttpCompleteCallback.Request.class,
                HttpCallback.of(CallbackType.COMPLETE, CALLBACK_URI, "id").newRequest(CONTEXT));
        Assertions.assertInstanceOf(HttpStatusCallback.Request.class,
                HttpCallback.of(CallbackType.STATUS, CALLBACK_URI, "id").newRequest(CONTEXT));
        Assertions.assertInstanceOf(HttpForgetCallback.Request.class,
                HttpCallback.of(CallbackType.FORGET, CALLBACK_URI, "id").newRequest(CONTEXT));
        Assertions.assertInstanceOf(HttpAfterLRACallback.Request.class,
                HttpCallback.of(CallbackType.AFTER, CALLBACK_URI, "id").newRequest(CONTEXT));
    }

    @Test
    public void afterLRAUsesTheTransportNeutralEndStatus() {
        HttpCallback callback = HttpCallback.afterCallback(CALLBACK_URI, "com.example.Participant");

        HttpAfterLRACallback.Request request = (HttpAfterLRACallback.Request) callback.newRequest(
                new CallbackContext("http://localhost:8080/lra/1", null, null, null, "Closed"));

        Assertions.assertEquals(LRA.TransactionStatus.Closed, request.endStatus);
    }

    @Test
    public void serializesAsTheContractHttpType() throws Exception {
        HttpCallback callback = HttpCallback.completeCallback(CALLBACK_URI, "com.example.Participant");

        String json = callback.toJson();
        LRACallback decoded = LRACallback.fromJson(json);

        Assertions.assertTrue(json.contains("\"type\":\"http-contract\""));
        Assertions.assertInstanceOf(HttpCallback.class, decoded);
        Assertions.assertEquals(CALLBACK_URI.toASCIIString(), ((HttpCallback) decoded).getUri());
        Assertions.assertEquals(CallbackType.COMPLETE, ((HttpCallback) decoded).getOperation());
        Assertions.assertEquals("com.example.Participant", ((HttpCallback) decoded).getParticipantId());
    }

    @Test
    public void mapsContractReplies() {
        HttpCallback compensate = HttpCallback.compensateCallback(CALLBACK_URI, "com.example.Participant");
        HttpCallback status = HttpCallback.statusCallback(CALLBACK_URI, "com.example.Participant");
        HttpCallback forget = HttpCallback.forgetCallback(CALLBACK_URI, "com.example.Participant");

        CallbackResult compensated = compensate.mapReply("{\"status\":\"Compensated\"}");
        CallbackResult failed = compensate.mapReply("{\"status\":\"FailedToCompensate\"}");
        CallbackResult inProgress = status.mapReply("{\"status\":\"Compensating\"}");
        CallbackResult forgotten = forget.mapReply("{\"error\":\"boom\"}");

        Assertions.assertEquals(CallbackStatus.OK, compensated.getStatus());
        Assertions.assertEquals(CallbackStatus.FAILED, failed.getStatus());
        Assertions.assertEquals("FailedToCompensate", failed.getBody());
        Assertions.assertEquals(CallbackStatus.ACCEPTED, inProgress.getStatus());
        Assertions.assertEquals(CallbackStatus.ERROR, forgotten.getStatus());
        Assertions.assertEquals("boom", forgotten.getBody());
    }

    @Test
    public void rejectsNonContractReplies() {
        HttpCallback callback = HttpCallback.completeCallback(CALLBACK_URI, "com.example.Participant");

        Assertions.assertEquals(CallbackStatus.ERROR, callback.mapReply(null).getStatus());
        Assertions.assertEquals(CallbackStatus.ERROR, callback.mapReply("<html>failure</html>").getStatus());
    }

    @Test
    public void legacyHttpCallbacksKeepTheOriginalWireType() throws Exception {
        LegacyHttpCallback callback = LegacyHttpCallback.compensateCallback(CALLBACK_URI);

        String json = callback.toJson();
        LRACallback decoded = LRACallback.fromJson(json);

        Assertions.assertTrue(json.contains("\"type\":\"http\""));
        Assertions.assertInstanceOf(LegacyHttpCallback.class, decoded);
        Assertions.assertEquals(CALLBACK_URI.toASCIIString(), ((LegacyHttpCallback) decoded).getUri());
    }
}
