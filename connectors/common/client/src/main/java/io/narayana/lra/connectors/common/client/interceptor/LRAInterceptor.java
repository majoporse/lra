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
import java.util.concurrent.CompletionStage;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;

/**
 * CDI interceptor that manages the LRA lifecycle of an annotated method. It is the only driver of
 * the lifecycle: it reads {@code @LRA}, {@code @Leave} and {@code @PropagateToken}, asks the
 * transport for the inbound data, delegates the decisions to the lifecycle processor, and hands the
 * response phase back to the transport when the transport owns it.
 * <p>
 * For a method whose result only exists after a transport response (a JAX-RS resource method, whose
 * cancel rules are status codes) the transport stores the invocation and completes it from the
 * response. For a plain CDI bean the interceptor completes the lifecycle itself, using the
 * {@link Outcome} carried by a returned {@link Result} (or the exception the method threw).
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
@LRA
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

        boolean deferred = transport.deferCompletion(invocationContext, invocation);

        if (!deferred && isAsynchronous(method)) {
            throw new IllegalStateException(String.format(
                    "%s: an @LRA method returning CompletionStage is only supported when the transport "
                            + "completes the lifecycle after the method returns",
                    method.toGenericString()));
        }

        boolean aborted = invocation.isAborted();
        Object result = null;
        Throwable thrown = null;
        Outcome outcome = aborted ? Outcome.SERVER_ERROR : Outcome.SUCCESS;

        if (!aborted) {
            try {
                result = invocationContext.proceed();
                outcome = outcomeOf(result);
            } catch (Exception | Error e) {
                thrown = e;
                outcome = Outcome.SERVER_ERROR;
            }
        }

        if (deferred) {
            // the transport completes the lifecycle once its response (and therefore the cancel
            // decision) exists, and restores the thread-local state
            if (aborted) {
                throw abortException(invocation, null);
            }

            rethrow(thrown);
            return result;
        }

        LifecycleDecision decision;

        try {
            decision = processor.processResponse(invocation,
                    processor.isCancel(invocation, outcome, thrown != null || aborted));
        } finally {
            invocation.restoreCurrent();
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
            LRALogger.logger.error(decision.failureMessage());

            if (thrown != null) {
                thrown.addSuppressed(new LifecycleException(decision.failureMessage()));
            } else {
                throw new LifecycleException(decision.failureMessage());
            }
        }

        rethrow(thrown);
        return result;
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

    private static boolean isAsynchronous(Method method) {
        return method.getReturnType().equals(CompletionStage.class);
    }

    private Outcome outcomeOf(Object result) {
        if (result instanceof Result<?> wrapper) {
            Outcome outcome = wrapper.getOutcome();
            return outcome == null ? Outcome.SUCCESS : outcome;
        }

        return Outcome.SUCCESS;
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
