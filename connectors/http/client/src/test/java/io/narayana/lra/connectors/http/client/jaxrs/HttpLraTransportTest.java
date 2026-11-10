/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.jaxrs;

import io.narayana.lra.api.Outcome;
import io.narayana.lra.api.Result;
import io.narayana.lra.connectors.common.client.invocation.LraCompletion;
import io.narayana.lra.connectors.common.client.invocation.LraInvocation;
import io.narayana.lra.connectors.common.client.invocation.LraMethodMetadata;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.Method;
import java.util.Collections;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HttpLraTransportTest {

    private final HttpLraTransport transport = new HttpLraTransport();
    private final LraMethodMetadata metadata = new LraMethodMetadata(LRA.Type.REQUIRED, true, null,
            null, null, false, false, false);

    @Test
    public void mapsAResponseStatus() {
        LraCompletion completion = transport.completionOf(invocation("response"), Response.status(409).build(), null);

        Assertions.assertEquals(Outcome.CLIENT_ERROR, completion.outcome());
        Assertions.assertEquals(409, completion.statusCode());
        Assertions.assertTrue(completion.failed());
    }

    @Test
    public void mapsAWebApplicationException() {
        LraCompletion completion = transport.completionOf(invocation("response"), null,
                new WebApplicationException(Response.status(503).build()));

        Assertions.assertEquals(Outcome.SERVER_ERROR, completion.outcome());
        Assertions.assertEquals(503, completion.statusCode());
        Assertions.assertTrue(completion.failed());
    }

    @Test
    public void mapsAnotherExceptionToServerError() {
        LraCompletion completion = transport.completionOf(invocation("response"), null,
                new IllegalStateException("boom"));

        Assertions.assertEquals(Outcome.SERVER_ERROR, completion.outcome());
        Assertions.assertEquals(500, completion.statusCode());
        Assertions.assertEquals("boom", completion.message());
    }

    @Test
    public void mapsVoidToNoContent() {
        LraCompletion completion = transport.completionOf(invocation("voidResult"), null, null);

        Assertions.assertEquals(Outcome.SUCCESS, completion.outcome());
        Assertions.assertEquals(204, completion.statusCode());
        Assertions.assertFalse(completion.failed());
    }

    @Test
    public void mapsAnEntityToOk() {
        LraCompletion completion = transport.completionOf(invocation("entity"), "ok", null);

        Assertions.assertEquals(Outcome.SUCCESS, completion.outcome());
        Assertions.assertEquals(200, completion.statusCode());
    }

    @Test
    public void mapsANeutralResult() {
        LraCompletion completion = transport.completionOf(invocation("entity"),
                Result.clientError("bad"), null);

        Assertions.assertEquals(Outcome.CLIENT_ERROR, completion.outcome());
        Assertions.assertEquals(400, completion.statusCode());
        Assertions.assertFalse(completion.failed());
    }

    @Test
    public void mapsAnAbortedInvocationToItsStatus() {
        LraInvocation invocation = invocation("response");
        invocation.setAborted(true);
        invocation.setAbortStatusCode(412);
        invocation.setAbortMessage("MANDATORY but no tx");

        LraCompletion completion = transport.completionOf(invocation, null, null);

        Assertions.assertEquals(Outcome.CLIENT_ERROR, completion.outcome());
        Assertions.assertEquals(412, completion.statusCode());
        Assertions.assertEquals("MANDATORY but no tx", completion.message());
        Assertions.assertTrue(completion.failed());
    }

    private LraInvocation invocation(String methodName) {
        try {
            Method method = Resource.class.getDeclaredMethod(methodName);
            return new LraInvocation(method, Resource.class, metadata, Collections.emptyList(), null);
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    public static class Resource {

        public Response response() {
            return Response.ok().build();
        }

        public void voidResult() {
        }

        public String entity() {
            return "ok";
        }
    }
}
