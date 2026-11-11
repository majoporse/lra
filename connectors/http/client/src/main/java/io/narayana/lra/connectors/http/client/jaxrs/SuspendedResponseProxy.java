/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.jaxrs;

import io.narayana.lra.connectors.common.client.invocation.AsyncCompletion;
import io.narayana.lra.logging.LRALogger;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.TimeoutHandler;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * Observes the value an application resumes an {@code @Suspended} response with.
 * <p>
 * JAX-RS reports only failures to callbacks, so the only way to see a successful
 * {@code resume(value)} is to give the method a proxy. The proxy completes the LRA before delegating
 * the resume, so the transport's response phase can never supply the completion instead.
 * <p>
 * It installs a default {@link TimeoutHandler} equivalent to the container's own (resume with a 503
 * failure), but routed through the proxy, so a timeout is completed as well. A handler the
 * application installs replaces it and receives the proxy, so a handler-driven resume is observed
 * too.
 */
final class SuspendedResponseProxy implements InvocationHandler {

    private final AsyncResponse delegate;
    private final AsyncCompletion completion;

    private SuspendedResponseProxy(AsyncResponse delegate, AsyncCompletion completion) {
        this.delegate = delegate;
        this.completion = completion;
    }

    /**
     * @param delegate the response the container resolved for the method
     * @param carrierType the declared parameter type; a sub-interface of {@link AsyncResponse} is
     *        implemented by the proxy as well
     * @param completion the sink that completes the invocation
     */
    static AsyncResponse create(AsyncResponse delegate, Class<?> carrierType, AsyncCompletion completion) {
        SuspendedResponseProxy handler = new SuspendedResponseProxy(delegate, completion);
        Class<?>[] interfaces = carrierType.equals(AsyncResponse.class)
                ? new Class<?>[] { AsyncResponse.class }
                : new Class<?>[] { AsyncResponse.class, carrierType };
        AsyncResponse proxy = (AsyncResponse) Proxy.newProxyInstance(AsyncResponse.class.getClassLoader(),
                interfaces, handler);

        // the container's own timeout resume does not pass through this proxy; replace the default
        // handler with an equivalent one that does, so every resume has a completion
        delegate.setTimeoutHandler(response -> proxy.resume(cancelled()));

        return proxy;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getDeclaringClass() == Object.class) {
            return switch (method.getName()) {
                case "equals" -> proxy == args[0];
                case "hashCode" -> System.identityHashCode(proxy);
                default -> delegate.toString();
            };
        }

        switch (method.getName()) {
            case "resume" -> {
                Object value = args[0];

                // a Throwable is the failure overload; its value cannot be a response entity
                if (value instanceof Throwable throwable) {
                    return resumeFailure(throwable);
                }

                return resumeValue(value);
            }
            case "cancel" -> {
                // a cancelled request is a 503: let the normal cancel rules decide the LRA outcome
                if (delegate.isSuspended()) {
                    try {
                        completion.completed(null, cancelled());
                    } catch (RuntimeException | Error e) {
                        // the request must still be cancelled; the client sees 503 either way
                        LRALogger.logger.error(
                                "The LRA lifecycle could not be completed for a cancelled asynchronous response", e);
                    }
                }

                return invokeDelegate(method, args);
            }
            case "setTimeoutHandler" -> {
                TimeoutHandler requested = (TimeoutHandler) args[0];
                // "no handler" means the container's default timeout, which must still be observed
                TimeoutHandler effective = requested != null
                        ? requested
                        : ignored -> ((AsyncResponse) proxy).resume(cancelled());
                args[0] = (TimeoutHandler) response -> effective.handleTimeout((AsyncResponse) proxy);
            }
            default -> {
                // delegated unchanged
            }
        }

        return invokeDelegate(method, args);
    }

    private Object invokeDelegate(Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            // a dynamic proxy must not expose the reflective wrapper
            throw e.getCause();
        }
    }

    private boolean resumeValue(Object value) throws Throwable {
        if (delegate.isSuspended()) {
            try {
                completion.completed(value, null);
            } catch (RuntimeException | Error e) {
                // the completion failed: fail the response the way a failing stage would, instead of
                // resuming a value whose LRA was not completed
                return delegate.resume(e);
            }
        }

        return delegate.resume(value);
    }

    private boolean resumeFailure(Throwable failure) throws Throwable {
        if (delegate.isSuspended()) {
            try {
                completion.completed(null, failure);
            } catch (RuntimeException | Error e) {
                // the completion failed: report that instead of the application's failure
                return delegate.resume(e);
            }
        }

        return delegate.resume(failure);
    }

    private static WebApplicationException cancelled() {
        return new WebApplicationException(Response.status(Response.Status.SERVICE_UNAVAILABLE).build());
    }
}
