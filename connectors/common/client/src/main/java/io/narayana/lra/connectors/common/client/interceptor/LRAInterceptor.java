/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.interceptor;

import io.narayana.lra.Current;
import io.narayana.lra.api.LifecycleException;
import io.narayana.lra.api.Outcome;
import io.narayana.lra.api.Result;
import io.narayana.lra.connectors.common.client.LRAAbortException;
import io.narayana.lra.connectors.common.client.invocation.AsyncCompletion;
import io.narayana.lra.connectors.common.client.invocation.LraCompletion;
import io.narayana.lra.connectors.common.client.invocation.LraInvocation;
import io.narayana.lra.connectors.common.client.invocation.LraMethodMetadata;
import io.narayana.lra.connectors.common.client.lifecycle.LifecycleDecision;
import io.narayana.lra.connectors.common.client.lifecycle.LraLifecycleProcessor;
import io.narayana.lra.logging.LRALogger;
import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.Interceptor;
import jakarta.interceptor.InvocationContext;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

/**
 * CDI interceptor that manages the LRA lifecycle of an annotated method. It is the only driver of
 * the lifecycle: it reads {@code @LRA}, {@code @Leave} and {@code @PropagateToken}, asks the
 * transport for the inbound data, delegates the decisions to the lifecycle processor, and hands the
 * response phase back to the transport when the transport owns it.
 * <p>
 * It is bound through {@link LraManaged}, which the portable extension and the Quarkus build step add
 * to every method carrying {@code @LRA}, {@code @Leave} or a method-level {@code @PropagateToken};
 * {@code @LRA} itself is no longer the binding, so one interceptor can cover all three.
 * <p>
 * For a method whose result only exists after a transport response (a JAX-RS resource method, whose
 * cancel rules are status codes) the transport stores the invocation and completes it from the
 * response. For a plain CDI bean the interceptor completes the lifecycle itself, using the
 * {@link Outcome} carried by a returned {@link Result} (or the exception the method threw).
 * Asynchronous results are completed from the eventual result value: a returned
 * {@link CompletionStage} is completed by this interceptor, and a transport carrier (a JAX-RS
 * {@code @Suspended} response) is completed by the transport through {@link AsyncCompletion}.
 * <p>
 * The thread-local state found on entry is kept intact: the interceptor only undoes the changes made
 * by its own request phase, so a bean invoked from within another LRA participates in the enclosing
 * LRA without disturbing it. A lifecycle that completes with failed steps is surfaced to the caller
 * as a {@link LifecycleException} (attached as a suppressed exception when the business method
 * already threw); an invocation stopped before the method can run is reported with an
 * {@link LRAAbortException}.
 * <p>
 * A method carrying a participant callback annotation ({@code @Compensate}, {@code @Complete},
 * {@code @Status}, {@code @Forget} or {@code @AfterLRA}) is never treated as business work: the
 * interceptor proceeds immediately, because a class-level {@code @LRA} would otherwise intercept the
 * callbacks the coordinator invokes.
 */
@Interceptor
@LraManaged
@Priority(Interceptor.Priority.PLATFORM_BEFORE + 200)
public class LRAInterceptor {

    @Inject
    LRAAnnotationResolver resolver;

    @Inject
    LRATransport transport;

    @Inject
    LraLifecycleProcessor processor;

    @AroundInvoke
    public Object intercept(InvocationContext invocationContext) throws Exception {
        Method method = invocationContext.getMethod();
        LraMethodMetadata metadata = resolver.resolve(method);

        if (metadata.isParticipantCallback() || !metadata.isManaged()) {
            return invocationContext.proceed();
        }

        // capture the LRA context and credential that were in effect before this invocation so that
        // the cleanup can restore them (this invocation must not clobber the context of any
        // enclosing LRA processing)
        List<Object> contextsOnEntry = Current.getContexts();
        String authTokenOnEntry = Current.getAuthToken();

        if (metadata.isPropagateToken()) {
            propagateCredential(transport.credential());
        }

        LraInvocation invocation = new LraInvocation(method,
                resolveResourceClass(invocationContext, method), metadata, contextsOnEntry, authTokenOnEntry);
        invocation.setIncomingLRA(transport.incomingLRA());

        processor.processRequest(invocation);
        transport.bind(invocationContext, invocation);

        /*
         * The one completion routine for asynchronous results. A transport may call it from its own
         * carrier, the interceptor calls it from the returned stage. It runs on whichever thread
         * completes the invocation, so it reads only the invocation state and never the calling
         * thread's Current.
         */
        AsyncCompletion asyncCompletion = (value, failure) -> completeAsync(invocationContext, invocation, value,
                failure);

        boolean transportOwnsCompletion = transport.bindAsyncCompletion(invocationContext, invocation, asyncCompletion);

        boolean aborted = invocation.isAborted();
        Object result = null;
        Throwable thrown = null;

        if (!aborted) {
            try {
                result = invocationContext.proceed();
            } catch (Exception | Error e) {
                thrown = e;
            }
        }

        if (transportOwnsCompletion) {
            // the transport completes the lifecycle from its own carrier (a resumed @Suspended
            // AsyncResponse, for example)
            if (aborted) {
                throw abortException(invocation, null);
            }

            rethrow(thrown);
            return result;
        }

        if (thrown == null && result instanceof CompletionStage<?> stage) {
            /*
             * Return the stage the completion handler produces. The container subscribes to it, so
             * it cannot observe completion before the lifecycle decision is made (a dependent of
             * the same stage could otherwise run first and let the response phase supply the
             * completion). A completion failure fails that stage, which is how a synchronous
             * invocation reports it.
             */
            try {
                return stage.whenComplete(asyncCompletion::completed);
            } finally {
                // a transport without a response phase has no later request boundary, so the
                // thread that ran the request phase must not keep its LRA
                if (!transport.hasResponsePhase(invocationContext)) {
                    invocation.restoreCurrent();
                }
            }
        }

        return complete(invocationContext, invocation, aborted, result, thrown);
    }

    private Object complete(InvocationContext invocationContext, LraInvocation invocation, boolean aborted,
            Object result, Throwable thrown) throws Exception {
        LraCompletion completion = transport.completionOf(invocation, result, thrown);

        // the synchronous path owns the completion: claim it before the processor runs so that a
        // response-phase adapter cannot complete it as well
        invocation.tryMarkCompleted();

        LifecycleDecision decision;

        try {
            decision = processor.complete(invocation, completion);
        } finally {
            if (!transport.hasResponsePhase(invocationContext)) {
                // a response-phase transport owns the request-boundary cleanup
                invocation.restoreCurrent();
            }
        }

        if (aborted) {
            // the request processing stopped the invocation before the method could run
            throw abortException(invocation, decision.failureMessage());
        }

        /*
         * report any failed steps (ie if the LRA lifecycle completed with failures) to the
         * caller. The transport adapter writes the failure message into its response; a CDI
         * invocation has no response, so the interceptor logs the failure and throws it, or
         * attaches it to an exception the method itself threw.
         */
        if (decision.hasFailure()) {
            if (transport.hasResponsePhase(invocationContext)) {
                // the response adapter writes the failure into the response
                invocation.setLifecycleFailure(decision.failureMessage());
            } else {
                LRALogger.logger.error(decision.failureMessage());

                if (thrown != null) {
                    thrown.addSuppressed(new LifecycleException(decision.failureMessage()));
                } else {
                    throw new LifecycleException(decision.failureMessage());
                }
            }
        }

        rethrow(thrown);
        return result;
    }

    private void completeAsync(InvocationContext invocationContext, LraInvocation invocation, Object value,
            Throwable thrown) {
        Throwable failure = unwrap(thrown);

        if (!invocation.tryMarkCompleted()) {
            // the transport's response phase (or another completion) already owns it
            return;
        }

        LraCompletion completion = transport.completionOf(invocation, failure == null ? value : null, failure);
        LifecycleDecision decision = processor.complete(invocation, completion);

        if (decision.hasFailure()) {
            if (transport.hasResponsePhase(invocationContext)) {
                // the response filter renders the failure into the response
                invocation.setLifecycleFailure(decision.failureMessage());
            } else {
                // there is no response to report it in, and the caller already holds the result
                LRALogger.logger.error(decision.failureMessage());
            }
        }
    }

    private void propagateCredential(String token) {
        if (token != null) {
            Current.setAuthToken(token);
        }
    }

    private LRAAbortException abortException(LraInvocation invocation, String failureMessage) {
        String message = failureMessage != null ? failureMessage : invocation.getAbortMessage();

        return new LRAAbortException(invocation.getAbortStatusCode(), message);
    }

    private static Throwable unwrap(Throwable thrown) {
        Throwable failure = thrown;

        // a stage may wrap the failure of the value it completed with: map the same exception the
        // synchronous path would have seen
        while (failure != null
                && (failure instanceof CompletionException || failure instanceof ExecutionException)
                && failure.getCause() != null) {
            failure = failure.getCause();
        }

        return failure;
    }

    private Class<?> resolveResourceClass(InvocationContext invocationContext, Method method) {
        Class<?> declaringClass = method.getDeclaringClass();

        if (!declaringClass.isInterface() && !Modifier.isAbstract(declaringClass.getModifiers())) {
            return declaringClass;
        }

        Object target = invocationContext.getTarget();
        Class<?> targetClass = target == null ? declaringClass : target.getClass();
        Class<?> superClass = targetClass.getSuperclass();

        // a normal-scoped bean is invoked through a generated client proxy; the participant model is
        // keyed by the bean class, not the proxy class
        return superClass != null && superClass != Object.class ? superClass : targetClass;
    }

    private static void rethrow(Throwable thrown) throws Exception {
        if (thrown == null) {
            return;
        }

        if (thrown instanceof Error error) {
            throw error;
        }

        throw (Exception) thrown;
    }
}
