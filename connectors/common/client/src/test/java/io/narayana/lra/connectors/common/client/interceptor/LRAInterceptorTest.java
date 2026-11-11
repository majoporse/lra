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
import io.narayana.lra.connectors.common.client.invocation.AsyncCompletion;
import io.narayana.lra.connectors.common.client.invocation.LraCompletion;
import io.narayana.lra.connectors.common.client.invocation.LraInvocation;
import io.narayana.lra.connectors.common.client.invocation.LraMethodMetadata;
import io.narayana.lra.connectors.common.client.lifecycle.LifecycleDecision;
import io.narayana.lra.connectors.common.client.lifecycle.LraLifecycleProcessor;
import jakarta.interceptor.InvocationContext;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
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
        Bean.stage = null;
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
    public void leavesLifecycleFailuresToTheResponsePhase() throws Exception {
        processor.decision = new LifecycleDecision(false, 0, null, "close failed");
        transport.responsePhase = true;

        Object result = interceptor().intercept(invocation("work"));

        Assertions.assertEquals("ok", result);
        Assertions.assertNotNull(transport.boundInvocation);
        Assertions.assertEquals("close failed", transport.boundInvocation.getLifecycleFailure());
        Assertions.assertTrue(transport.boundInvocation.isCompleted());
    }

    @Test
    public void handsTheResponsePhaseToTheTransportWhenItBindsAnAsyncCarrier() throws Exception {
        transport.defer = true;

        Object result = interceptor().intercept(invocation("work"));

        Assertions.assertEquals("ok", result);
        Assertions.assertTrue(processor.requestCalled);
        Assertions.assertFalse(processor.responseCalled);
        Assertions.assertNotNull(transport.deferredInvocation);
        Assertions.assertNotNull(transport.asyncCompletion);
    }

    @Test
    public void completesFromTheEventualValueOfAStage() throws Exception {
        Bean.stage = new CompletableFuture<>();

        Object result = interceptor().intercept(invocation("asyncWork"));

        Assertions.assertNotSame(Bean.stage, result, "the returned stage must not complete before the LRA decision");
        Assertions.assertFalse(processor.responseCalled, "the lifecycle must not complete before the stage");

        Bean.stage.complete("done");

        Assertions.assertTrue(processor.responseCalled);
        Assertions.assertEquals("done", transport.completedValue);
        Assertions.assertNull(transport.completedThrown);
    }

    @Test
    public void completesFromANullStageValue() throws Exception {
        Bean.stage = CompletableFuture.completedFuture(null);

        interceptor().intercept(invocation("asyncWork"));

        Assertions.assertTrue(processor.responseCalled);
        Assertions.assertNull(transport.completedValue);
        Assertions.assertNull(transport.completedThrown);
    }

    @Test
    public void unwrapsTheFailureOfAStage() throws Exception {
        Bean.stage = new CompletableFuture<>();
        Bean.stage.completeExceptionally(new CompletionException(new IllegalStateException("boom")));

        interceptor().intercept(invocation("asyncWork"));

        Assertions.assertInstanceOf(IllegalStateException.class, transport.completedThrown,
                "the transport must see the exception the method completed with");
        Assertions.assertEquals("boom", transport.completedThrown.getMessage());
    }

    @Test
    public void unwrapsExecutionExceptionsToo() throws Exception {
        Bean.stage = new CompletableFuture<>();
        Bean.stage.completeExceptionally(new ExecutionException(new IllegalArgumentException("bad")));

        interceptor().intercept(invocation("asyncWork"));

        Assertions.assertInstanceOf(IllegalArgumentException.class, transport.completedThrown);
        Assertions.assertEquals("bad", transport.completedThrown.getMessage());
    }

    @Test
    public void doesNotCompleteWhenTheTransportAlreadyClaimedIt() throws Exception {
        transport.claimOnBind = true;
        Bean.stage = CompletableFuture.completedFuture("done");

        interceptor().intercept(invocation("asyncWork"));

        Assertions.assertFalse(processor.responseCalled,
                "the transport's response phase already completed the invocation");
    }

    @Test
    public void restoresCurrentWhenAPlainBeanReturnsAStage() throws Exception {
        processor.onRequest = context -> {
            URI lra = URI.create("http://localhost:8080/lra-coordinator/" + UUID.randomUUID());
            context.setCurrentLRA(lra);
            Current.push(lra, null);
            Current.addActiveLRACache(lra);
        };
        Bean.stage = new CompletableFuture<>();

        interceptor().intercept(invocation("asyncWork"));

        Assertions.assertNull(Current.peek(), "the request thread must not keep the LRA of a returned stage");
    }

    @Test
    public void completesAnAsynchronousInvocationExactlyOnce() throws Exception {
        CompletableFuture<Object> stage = new CompletableFuture<>();
        Bean.stage = stage;

        interceptor().intercept(invocation("asyncWork"));

        stage.complete("done");
        stage.complete("again");
        transport.asyncCompletion.completed("late", null);

        Assertions.assertEquals(1, processor.responseCalls);
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

        static CompletableFuture<Object> stage;

        boolean invoked;

        @LRA
        public String work() {
            return "ok";
        }

        @LRA
        public CompletionStage<Object> asyncWork() {
            return stage;
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
        public LraMethodMetadata resolve(Method method) {
            LRA.Type propagation = method.isAnnotationPresent(LRA.class) ? LRA.Type.REQUIRED : null;
            boolean propagateToken = method.isAnnotationPresent(PropagateToken.class);

            return new LraMethodMetadata(propagation, propagation != null, null, null, null, false,
                    false, propagateToken);
        }
    }

    private static final class StubTransport implements LRATransport {

        boolean bindCalled;
        boolean deferred;
        boolean defer;
        boolean claimOnBind;
        boolean responsePhase;
        String credential;
        LraInvocation boundInvocation;
        LraInvocation deferredInvocation;
        AsyncCompletion asyncCompletion;
        Object completedValue;
        Throwable completedThrown;

        @Override
        public URI incomingLRA() {
            return null;
        }

        @Override
        public String credential() {
            return credential;
        }

        @Override
        public void bind(InvocationContext invocation, LraInvocation lraInvocation) {
            bindCalled = true;
            boundInvocation = lraInvocation;
        }

        @Override
        public boolean hasResponsePhase(InvocationContext invocation) {
            return responsePhase;
        }

        @Override
        public LraCompletion completionOf(LraInvocation lraInvocation, Object result, Throwable thrown) {
            completedValue = result;
            completedThrown = thrown;

            return thrown != null ? LraCompletion.failure(thrown.getMessage()) : LraCompletion.success();
        }

        @Override
        public boolean bindAsyncCompletion(InvocationContext invocation, LraInvocation lraInvocation,
                AsyncCompletion completion) {
            deferred = defer;
            asyncCompletion = completion;

            if (defer) {
                deferredInvocation = lraInvocation;
            }

            if (claimOnBind) {
                lraInvocation.tryMarkCompleted();
            }

            return defer;
        }
    }

    private static final class StubProcessor implements LraLifecycleProcessor {

        boolean requestCalled;
        boolean responseCalled;
        int responseCalls;
        Consumer<LraInvocation> onRequest = lraInvocation -> {
        };
        LifecycleDecision decision = new LifecycleDecision(false, 0, null, null);

        @Override
        public void processRequest(LraInvocation lraInvocation) {
            requestCalled = true;
            onRequest.accept(lraInvocation);
        }

        @Override
        public LifecycleDecision processResponse(LraInvocation lraInvocation, boolean cancel) {
            responseCalled = true;
            responseCalls++;

            return decision;
        }

        @Override
        public boolean isCancel(LraInvocation lraInvocation, Outcome outcome, boolean failed) {
            return false;
        }

        @Override
        public boolean isCancel(LraInvocation lraInvocation, int statusCode) {
            return false;
        }
    }
}
