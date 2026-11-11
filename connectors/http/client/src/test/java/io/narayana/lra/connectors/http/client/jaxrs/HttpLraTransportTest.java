/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.jaxrs;

import io.narayana.lra.api.Outcome;
import io.narayana.lra.api.Result;
import io.narayana.lra.connectors.common.client.invocation.AsyncCompletion;
import io.narayana.lra.connectors.common.client.invocation.LraCompletion;
import io.narayana.lra.connectors.common.client.invocation.LraInvocation;
import io.narayana.lra.connectors.common.client.invocation.LraMethodMetadata;
import jakarta.interceptor.InvocationContext;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.container.TimeoutHandler;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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

    @Test
    public void observesTheValueASuspendedResponseResumesWith() throws Exception {
        Method method = Resource.class.getDeclaredMethod("suspended", AsyncResponse.class);
        FakeAsyncResponse real = new FakeAsyncResponse();
        InvocationContext context = invocationContext(method, real);
        List<Completion> completions = new ArrayList<>();

        Assertions.assertTrue(transport.bindAsyncCompletion(context, invocation(method), capture(completions)));

        AsyncResponse proxy = (AsyncResponse) context.getParameters()[0];
        Assertions.assertNotSame(real, proxy);
        Assertions.assertEquals(proxy, proxy);
        Assertions.assertNotEquals(proxy, real);

        Assertions.assertTrue(proxy.resume("ok"));

        Assertions.assertEquals("ok", real.resumed, "the original resume must still happen");
        Assertions.assertEquals(1, completions.size());
        Assertions.assertEquals("ok", completions.get(0).value());
        Assertions.assertNull(completions.get(0).thrown());
    }

    @Test
    public void observesTheFailureASuspendedResponseResumesWith() throws Exception {
        Method method = Resource.class.getDeclaredMethod("suspended", AsyncResponse.class);
        FakeAsyncResponse real = new FakeAsyncResponse();
        InvocationContext context = invocationContext(method, real);
        List<Completion> completions = new ArrayList<>();
        Throwable failure = new IllegalStateException("boom");

        transport.bindAsyncCompletion(context, invocation(method), capture(completions));
        AsyncResponse proxy = (AsyncResponse) context.getParameters()[0];

        Assertions.assertTrue(proxy.resume(failure));

        Assertions.assertSame(failure, real.resumedFailure);
        Assertions.assertSame(failure, completions.get(0).thrown());
    }

    @Test
    public void mapsASuspendedCancelAsServiceUnavailable() throws Exception {
        Method method = Resource.class.getDeclaredMethod("suspended", AsyncResponse.class);
        FakeAsyncResponse real = new FakeAsyncResponse();
        InvocationContext context = invocationContext(method, real);
        List<Completion> completions = new ArrayList<>();

        transport.bindAsyncCompletion(context, invocation(method), capture(completions));
        AsyncResponse proxy = (AsyncResponse) context.getParameters()[0];

        Assertions.assertTrue(proxy.cancel());

        Assertions.assertTrue(real.cancelled);
        Assertions.assertInstanceOf(WebApplicationException.class, completions.get(0).thrown());
        Assertions.assertEquals(503,
                ((WebApplicationException) completions.get(0).thrown()).getResponse().getStatus());
    }

    @Test
    public void handsTheTimeoutHandlerTheProxy() throws Exception {
        Method method = Resource.class.getDeclaredMethod("suspended", AsyncResponse.class);
        FakeAsyncResponse real = new FakeAsyncResponse();
        InvocationContext context = invocationContext(method, real);
        List<Completion> completions = new ArrayList<>();

        transport.bindAsyncCompletion(context, invocation(method), capture(completions));
        AsyncResponse proxy = (AsyncResponse) context.getParameters()[0];

        List<AsyncResponse> handled = new ArrayList<>();
        proxy.setTimeoutHandler(response -> {
            handled.add(response);
            response.resume("timeout");
        });

        Assertions.assertNotNull(real.timeoutHandler);
        // the container invokes the handler with its own instance
        real.timeoutHandler.handleTimeout(real);

        Assertions.assertSame(proxy, handled.get(0), "a handler-driven resume must complete the LRA");
        Assertions.assertEquals("timeout", completions.get(0).value());
        Assertions.assertEquals("timeout", real.resumed);
    }

    @Test
    public void observesTheDefaultTimeout() throws Exception {
        Method method = Resource.class.getDeclaredMethod("suspended", AsyncResponse.class);
        FakeAsyncResponse real = new FakeAsyncResponse();
        InvocationContext context = invocationContext(method, real);
        List<Completion> completions = new ArrayList<>();

        transport.bindAsyncCompletion(context, invocation(method), capture(completions));

        Assertions.assertNotNull(real.timeoutHandler, "a timeout must not bypass the completion");

        // the container times out and invokes the installed handler with its own instance
        real.timeoutHandler.handleTimeout(real);

        Assertions.assertEquals(1, completions.size());
        Assertions.assertInstanceOf(WebApplicationException.class, completions.get(0).thrown());
        Assertions.assertEquals(503,
                ((WebApplicationException) completions.get(0).thrown()).getResponse().getStatus());
        Assertions.assertInstanceOf(WebApplicationException.class, real.resumedFailure);
    }

    @Test
    public void keepsTheDefaultTimeoutWhenTheApplicationClearsTheHandler() throws Exception {
        Method method = Resource.class.getDeclaredMethod("suspended", AsyncResponse.class);
        FakeAsyncResponse real = new FakeAsyncResponse();
        InvocationContext context = invocationContext(method, real);
        List<Completion> completions = new ArrayList<>();

        transport.bindAsyncCompletion(context, invocation(method), capture(completions));
        AsyncResponse proxy = (AsyncResponse) context.getParameters()[0];

        proxy.setTimeoutHandler(null);
        real.timeoutHandler.handleTimeout(real);

        Assertions.assertEquals(1, completions.size());
        Assertions.assertEquals(503,
                ((WebApplicationException) completions.get(0).thrown()).getResponse().getStatus());
    }

    @Test
    public void doesNotCompleteWhenTheResponseIsNoLongerSuspended() throws Exception {
        Method method = Resource.class.getDeclaredMethod("suspended", AsyncResponse.class);
        FakeAsyncResponse real = new FakeAsyncResponse();
        real.suspended = false;
        InvocationContext context = invocationContext(method, real);
        List<Completion> completions = new ArrayList<>();

        transport.bindAsyncCompletion(context, invocation(method), capture(completions));
        AsyncResponse proxy = (AsyncResponse) context.getParameters()[0];

        proxy.resume("late");

        Assertions.assertTrue(completions.isEmpty(), "a resume the container ignored must not complete the LRA");
        Assertions.assertEquals("late", real.resumed);
    }

    @Test
    public void doesNotOwnTheCompletionWithoutASuspendedParameter() throws Exception {
        Method method = Resource.class.getMethod("entity");

        Assertions.assertFalse(transport.bindAsyncCompletion(invocationContext(method), invocation(method),
                (value, thrown) -> {
                }));
    }

    private static AsyncCompletion capture(List<Completion> completions) {
        return (value, thrown) -> completions.add(new Completion(value, thrown));
    }

    private static InvocationContext invocationContext(Method method, Object... parameters) {
        AtomicReference<Object[]> holder = new AtomicReference<>(parameters);

        return (InvocationContext) Proxy.newProxyInstance(HttpLraTransportTest.class.getClassLoader(),
                new Class<?>[] { InvocationContext.class }, (proxy, invoked, args) -> switch (invoked.getName()) {
                    case "getMethod" -> method;
                    case "getParameters" -> holder.get();
                    case "setParameters" -> {
                        holder.set((Object[]) args[0]);
                        yield null;
                    }
                    default -> null;
                });
    }

    private LraInvocation invocation(String methodName) {
        try {
            return invocation(Resource.class.getDeclaredMethod(methodName));
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    private LraInvocation invocation(Method method) {
        return new LraInvocation(method, Resource.class, metadata, Collections.emptyList(), null);
    }

    private record Completion(Object value, Throwable thrown) {
    }

    @Path("/test")
    public static class Resource {

        public Response response() {
            return Response.ok().build();
        }

        public void voidResult() {
        }

        public String entity() {
            return "ok";
        }

        public void suspended(@Suspended AsyncResponse response) {
        }
    }

    private static final class FakeAsyncResponse implements AsyncResponse {

        Object resumed;
        Throwable resumedFailure;
        boolean cancelled;
        boolean suspended = true;
        TimeoutHandler timeoutHandler;

        @Override
        public boolean resume(Object response) {
            resumed = response;
            suspended = false;

            return true;
        }

        @Override
        public boolean resume(Throwable response) {
            resumedFailure = response;
            suspended = false;

            return true;
        }

        @Override
        public boolean cancel() {
            cancelled = true;
            suspended = false;

            return true;
        }

        @Override
        public boolean cancel(int retryAfter) {
            return cancel();
        }

        @Override
        public boolean cancel(Date retryAfter) {
            return cancel();
        }

        @Override
        public boolean isSuspended() {
            return suspended;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isDone() {
            return !suspended;
        }

        @Override
        public boolean setTimeout(long time, TimeUnit unit) {
            return suspended;
        }

        @Override
        public void setTimeoutHandler(TimeoutHandler handler) {
            timeoutHandler = handler;
        }

        @Override
        public Collection<Class<?>> register(Class<?> callback) {
            return List.of();
        }

        @Override
        public Map<Class<?>, Collection<Class<?>>> register(Class<?> callback, Class<?>... callbacks) {
            return Map.of();
        }

        @Override
        public Collection<Class<?>> register(Object callback) {
            return List.of();
        }

        @Override
        public Map<Class<?>, Collection<Class<?>>> register(Object callback, Object... callbacks) {
            return Map.of();
        }
    }
}
