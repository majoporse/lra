/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.interceptor;

import io.narayana.lra.Current;
import io.narayana.lra.PropagateToken;
import io.narayana.lra.api.LifecycleException;
import io.narayana.lra.api.Outcome;
import io.narayana.lra.connectors.common.client.LRAAbortException;
import io.narayana.lra.connectors.common.client.invocation.LraInvocation;
import io.narayana.lra.connectors.common.client.invocation.ServerLRAContext;
import io.narayana.lra.connectors.common.client.lifecycle.LifecycleDecision;
import io.narayana.lra.connectors.common.client.lifecycle.LraLifecycleProcessor;
import jakarta.interceptor.InvocationContext;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.Collections;
import java.util.function.Consumer;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LRAInterceptorTest {

    private final Bean bean = new Bean();
    private final StubTransport transport = new StubTransport();
    private final StubProcessor processor = new StubProcessor();

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
    public void drivesTheRequestAndResponsePhasesForAPlainBean() throws Exception {
        Object result = interceptor().intercept(invocation("work"));

        Assertions.assertEquals("ok", result);
        Assertions.assertTrue(processor.requestCalled);
        Assertions.assertTrue(transport.bindCalled);
        Assertions.assertTrue(processor.responseCalled);
        Assertions.assertFalse(transport.deferred);
    }

    @Test
    public void surfacesALifecycleFailure() {
        processor.decision = new LifecycleDecision(false, 0, null, "close failed");

        LifecycleException thrown = Assertions.assertThrows(LifecycleException.class,
                () -> interceptor().intercept(invocation("work")));

        Assertions.assertEquals("close failed", thrown.getMessage());
    }

    @Test
    public void handsTheResponsePhaseToTheTransportWhenItDefers() throws Exception {
        transport.defer = true;

        Object result = interceptor().intercept(invocation("work"));

        Assertions.assertEquals("ok", result);
        Assertions.assertTrue(processor.requestCalled);
        Assertions.assertFalse(processor.responseCalled);
        Assertions.assertNotNull(transport.deferredInvocation);
    }

    @Test
    public void abortsWithoutInvokingTheBusinessMethod() {
        processor.onRequest = context -> {
            context.setAborted(true);
            context.setAbortStatusCode(412);
            context.setAbortMessage("MANDATORY but no tx");
        };
        transport.defer = true;

        LRAAbortException thrown = Assertions.assertThrows(LRAAbortException.class,
                () -> interceptor().intercept(invocation("work")));

        Assertions.assertEquals(412, thrown.getStatusCode());
        Assertions.assertFalse(bean.invoked);
    }

    @Test
    public void publishesTheCredentialForTheDurationOfTheInvocation() throws Exception {
        // the transport is responsible for extracting a credential from its carrier and for
        // rejecting values that are not token-shaped; the interceptor only publishes it
        transport.credential = "header.payload.signature";

        Object result = interceptor().intercept(invocation("tokenWork"));

        Assertions.assertEquals("header.payload.signature", result);
        Assertions.assertNull(Current.getAuthToken(), "the entry state must be restored");
    }

    private LRAInterceptor interceptor() {
        LRAInterceptor interceptor = new LRAInterceptor();
        interceptor.resolver = new StubResolver();
        interceptor.transport = transport;
        interceptor.processor = processor;
        return interceptor;
    }

    private InvocationContext invocation(String methodName) throws NoSuchMethodException {
        Method method = Bean.class.getMethod(methodName);

        return (InvocationContext) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] { InvocationContext.class }, (proxy, invoked, arguments) -> switch (invoked.getName()) {
                    case "getMethod" -> method;
                    case "getTarget" -> bean;
                    case "getParameters" -> new Object[0];
                    case "setParameters" -> null;
                    case "getContextData" -> Collections.emptyMap();
                    case "proceed" -> {
                        bean.invoked = true;
                        yield bean.invoke(method);
                    }
                    default -> null;
                });
    }

    public static class Bean {

        boolean invoked;

        @LRA
        public String work() {
            return "ok";
        }

        @PropagateToken
        public String tokenWork() {
            return Current.getAuthToken();
        }

        Object invoke(Method method) throws Exception {
            return method.invoke(this);
        }
    }

    private static final class StubResolver implements LRAAnnotationResolver {

        @Override
        public io.narayana.lra.connectors.common.client.invocation.LRATransactionMetadata resolveTransaction(
                Method method) {
            return null;
        }

        @Override
        public boolean isLeave(Method method) {
            return false;
        }

        @Override
        public boolean isParticipantCallback(Method method) {
            return false;
        }

        @Override
        public boolean propagateToken(Method method) {
            return method.isAnnotationPresent(PropagateToken.class);
        }
    }

    private static final class StubTransport implements LRATransportContext {

        boolean bindCalled;
        boolean deferred;
        boolean defer;
        String credential;
        LraInvocation deferredInvocation;

        @Override
        public URI incomingLRA() {
            return null;
        }

        @Override
        public String credential() {
            return credential;
        }

        @Override
        public void bind(InvocationContext invocation, ServerLRAContext context) {
            bindCalled = true;
        }

        @Override
        public boolean deferCompletion(InvocationContext invocation, LraInvocation lraInvocation) {
            deferred = defer;

            if (defer) {
                deferredInvocation = lraInvocation;
            }

            return defer;
        }
    }

    private static final class StubProcessor implements LraLifecycleProcessor {

        boolean requestCalled;
        boolean responseCalled;
        Consumer<ServerLRAContext> onRequest = context -> {
        };
        LifecycleDecision decision = new LifecycleDecision(false, 0, null, null);

        @Override
        public void processRequest(ServerLRAContext context) {
            requestCalled = true;
            onRequest.accept(context);
        }

        @Override
        public LifecycleDecision processResponse(ServerLRAContext context, boolean cancel) {
            responseCalled = true;
            return decision;
        }

        @Override
        public boolean isCancel(ServerLRAContext context, Outcome outcome, boolean failed) {
            return false;
        }

        @Override
        public boolean isCancel(ServerLRAContext context, int statusCode) {
            return false;
        }
    }
}
