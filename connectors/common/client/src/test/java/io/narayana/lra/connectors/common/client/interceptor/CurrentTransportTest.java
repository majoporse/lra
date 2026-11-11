/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.interceptor;

import io.narayana.lra.api.Outcome;
import io.narayana.lra.api.Result;
import io.narayana.lra.connectors.common.client.invocation.LraCompletion;
import io.narayana.lra.connectors.common.client.invocation.LraInvocationState;
import io.narayana.lra.connectors.common.client.invocation.LraMethodMetadata;
import java.lang.reflect.Method;
import java.util.Collections;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CurrentTransportTest {

    private final CurrentTransport transport = new CurrentTransport();
    private final LraMethodMetadata metadata = new LraMethodMetadata(LRA.Type.REQUIRED, true, null,
            null, null, false, false, false);

    @Test
    public void mapsANeutralResult() {
        LraCompletion completion = transport.completionOf(invocation(), Result.serverError("boom"), null);

        Assertions.assertEquals(Outcome.SERVER_ERROR, completion.outcome());
        Assertions.assertNull(completion.statusCode());
        Assertions.assertFalse(completion.failed());
    }

    @Test
    public void mapsAnException() {
        LraCompletion completion = transport.completionOf(invocation(), null, new IllegalStateException("boom"));

        Assertions.assertEquals(Outcome.SERVER_ERROR, completion.outcome());
        Assertions.assertEquals("boom", completion.message());
        Assertions.assertTrue(completion.failed());
    }

    @Test
    public void mapsASuccessfulResult() {
        LraCompletion completion = transport.completionOf(invocation(), "ok", null);

        Assertions.assertEquals(Outcome.SUCCESS, completion.outcome());
        Assertions.assertFalse(completion.failed());
    }

    private LraInvocationState invocation() {
        try {
            Method method = Bean.class.getDeclaredMethod("work");
            return new LraInvocationState(method, Bean.class, metadata, Collections.emptyList(), null);
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    public static class Bean {
        public String work() {
            return "ok";
        }
    }
}
