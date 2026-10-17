/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.filter.interceptor;

import static io.narayana.lra.LRAConstants.ENLIST_PARTICIPANT_CLIENT_MAX_RETRY;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;

import io.narayana.lra.AnnotationResolver;
import io.narayana.lra.Current;
import io.narayana.lra.LRAConstants;
import io.narayana.lra.api.LRA;
import io.narayana.lra.client.LRAParticipantData;
import io.narayana.lra.client.internal.proxy.nonjaxrs.LRAParticipantRegistry;
import io.narayana.lra.filter.core.ServerLRAContext;
import io.narayana.lra.filter.core.ServerLRAProcessor;
import io.narayana.lra.filter.resolver.CDILRAAnnotationResolver;
import io.narayana.lra.logging.LRALogger;
import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.Interceptor;
import jakarta.interceptor.InvocationContext;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * CDI interceptor that manages the LRA lifecycle of a plain (non-JAX-RS) CDI bean method, in the
 * same way that {@link io.narayana.lra.filter.ServerLRAFilter} does for JAX-RS resources.
 * <p>
 * It is the transport-neutral counterpart of the JAX-RS filter: instead of translating between LRA
 * state and HTTP request/response it translates between LRA state and a CDI
 * {@link InvocationContext}. The annotation handling itself is delegated to the shared
 * {@link ServerLRAProcessor}, driven by the transport-neutral {@code io.narayana.lra.api}
 * annotations.
 * <p>
 * The incoming LRA context is obtained from an injected {@link LRATransportContext} and the outcome
 * used to close or cancel the LRA is derived from the {@link LRA.Result} returned by the method (and
 * from any thrown exception). Methods that want to initiate or join an LRA are annotated with
 * {@link LRA.Transaction} and, optionally, {@link LRA.CancelOn}.
 * <p>
 * The interceptor keeps the thread-local state it found on entry intact: it only undoes the
 * changes made by its own request phase, so that a bean invoked from within another LRA (nested
 * bean calls) participates in the enclosing LRA without disturbing it. When the LRA lifecycle
 * itself completes with failed steps the failure is logged and surfaced to the caller as an
 * {@link LRA.LifecycleException} (attached as a suppressed exception when the business method
 * already threw). An invocation that is stopped before the method can run is reported with an
 * {@link LRAAbortException} instead.
 * <p>
 * A bean that also declares participant callbacks ({@link LRA.Compensate},
 * {@link LRA.Complete}, {@link LRA.Status}, {@link LRA.Forget} or {@link LRA.AfterLRA}) is
 * enlisted as a participant through the {@code lra-participant-proxy}.
 */
@Interceptor
@LRA.Transaction
@Priority(Interceptor.Priority.PLATFORM_BEFORE + 200)
public class LRAInterceptor {

    @Inject
    private LRAParticipantRegistry lraParticipantRegistry;

    @Inject
    LRAParticipantData data;

    @Inject
    LRATransportContext transportContext;

    @Inject
    @ConfigProperty(name = ENLIST_PARTICIPANT_CLIENT_MAX_RETRY, defaultValue = "3")
    int enlistMaxRetries;

    private volatile ServerLRAProcessor processor;

    @AroundInvoke
    public Object intercept(InvocationContext invocationContext) throws Exception {
        if (!LRAConstants.isInterceptorMode()) {
            // LRA processing is delegated to the legacy ServerLRAFilter (the default mode)
            LRALogger.logger.warnf("LRAInterceptor is disabled because %s is not set to true; "
                    + "the @LRA.Transaction method is running without LRA management",
                    LRAConstants.SERVER_FILTER_INTERCEPTOR_PROPERTY_NAME);
            return invocationContext.proceed();
        }

        // capture the LRA context and auth token that were in effect before this invocation
        // so that the cleanup can restore them (this invocation must not clobber the context
        // of any enclosing LRA processing)
        List<Object> contextsOnEntry = Current.getContexts();
        String authTokenOnEntry = Current.getAuthToken();

        Method method = invocationContext.getMethod();
        Class<?> resourceClass = resolveResourceClass(invocationContext, method);

        ServerLRAContext context = new ServerLRAContext(method, resourceClass);

        if (populateContext(context)) {
            getProcessor().processRequest(context);
        }

        boolean aborted = context.isAborted();
        Object result = null;
        Throwable thrown = null;
        LRA.Outcome outcome = aborted ? LRA.Outcome.SERVER_ERROR : LRA.Outcome.SUCCESS;

        if (!aborted) {
            try {
                result = invocationContext.proceed();
                outcome = outcomeOf(result);
            } catch (Exception | Error e) {
                thrown = e;
                outcome = LRA.Outcome.SERVER_ERROR;
            }
        }

        try {
            getProcessor().processResponse(context, isCancel(context, outcome, thrown != null || aborted));
        } finally {
            cleanupContext(context, contextsOnEntry, authTokenOnEntry);
        }

        if (aborted) {
            // the request processing stopped the invocation before the method could run
            throw createAbortException(context);
        }

        /*
         * report any failed steps (ie if the LRA lifecycle completed with failures) to the
         * caller. The old filter wrote the failure message into the response entity; a CDI
         * invocation has no response, so the interceptor logs the failure and throws it, or
         * attaches it to an exception the method itself threw.
         */
        String failureMessage = getProcessor().processFailures(context);

        if (failureMessage != null) {
            LRALogger.logger.error(failureMessage);

            if (thrown != null) {
                thrown.addSuppressed(new LRA.LifecycleException(failureMessage));
            } else {
                throw new LRA.LifecycleException(failureMessage);
            }
        }

        if (thrown != null) {
            if (thrown instanceof Error) {
                throw (Error) thrown;
            }

            throw (Exception) thrown;
        }

        return result;
    }

    // translate the transport state of the invocation into the context handed to the processor
    private boolean populateContext(ServerLRAContext context) {
        context.setAuthHeader(transportContext.getHeader(HttpHeaders.AUTHORIZATION));

        List<String> headers = transportContext.getHeaders(LRA_HTTP_CONTEXT_HEADER);

        if (headers != null && !headers.isEmpty()) {
            String header = Current.getLast(headers);

            try {
                context.setIncomingLRA(new URI(header));
            } catch (URISyntaxException e) {
                String msg = String.format("header %s contains an invalid URL %s", LRA_HTTP_CONTEXT_HEADER, header);
                Method method = context.getMethod();

                LRALogger.i18nLogger.warn_lraFilterContainerRequest(msg,
                        method.getDeclaringClass().getName() + "#" + method.getName(), "context");

                context.setAborted(true);
                context.setAbortStatusCode(Response.Status.PRECONDITION_FAILED.getStatusCode());
                context.setAbortMessage(msg);

                return false;
            }
        }

        context.setUriPrefix(() -> createUriPrefix(context.getResourceClass()));
        context.setBaseUri(transportContext.getBaseUri());

        return true;
    }

    private boolean isCancel(ServerLRAContext context, LRA.Outcome outcome, boolean failed) {
        LRA.CancelOn cancelOn = resolveCancelOn(context.getMethod());

        if (cancelOn == null) {
            // no explicit rule: close on normal completion and cancel when the method failed
            return failed;
        }

        for (LRA.Outcome candidate : cancelOn.value()) {
            if (candidate == outcome) {
                return true;
            }
        }

        return false;
    }

    private LRA.CancelOn resolveCancelOn(Method method) {
        LRA.CancelOn cancelOn = AnnotationResolver.resolveAnnotation(LRA.CancelOn.class, method);

        if (cancelOn == null) {
            cancelOn = method.getDeclaringClass().getAnnotation(LRA.CancelOn.class);
        }

        return cancelOn;
    }

    private LRA.Outcome outcomeOf(Object result) {
        if (result instanceof LRA.Result) {
            LRA.Outcome outcome = ((LRA.Result<?>) result).getOutcome();
            return outcome == null ? LRA.Outcome.SUCCESS : outcome;
        }

        return LRA.Outcome.SUCCESS;
    }

    // mirrors the base uri resolution performed by ServerLRAFilter#createUriPrefix
    private String createUriPrefix(Class<?> resourceClass) {
        String base = LRAConstants.getBaseUri().orElseGet(() -> {
            URI baseUri = transportContext.getBaseUri();
            return baseUri == null ? null : baseUri.toASCIIString();
        });

        if (base == null) {
            return null;
        }

        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }

        if (resourceClass.isAnnotationPresent(Path.class)) {
            String pathValue = resourceClass.getAnnotation(Path.class).value();
            return base + (pathValue.startsWith("/") ? pathValue : "/" + pathValue);
        }

        return base;
    }

    private LRAAbortException createAbortException(ServerLRAContext context) {
        String failure = context.getProgress() != null ? getProcessor().processFailures(context) : null;
        String message = failure != null ? failure : context.getAbortMessage();

        return new LRAAbortException(context.getAbortStatusCode(), message);
    }

    // restore the thread-local state that was in effect before this invocation: the LRA context
    // stack and the auth token. Unlike ServerLRAFilter, which brackets a whole HTTP request and
    // may clear the thread, the interceptor can be nested inside other LRA processing, so it only
    // undoes the changes made by its own request phase (and re-establishes an incoming LRA that
    // the processor suspended or removed).
    private void cleanupContext(ServerLRAContext context, List<Object> contextsOnEntry,
            String authTokenOnEntry) {
        if (context.getCurrentLRA() != null) {
            Current.removeActiveLRACache(context.getCurrentLRA());
        }

        // pop anything the request phase pushed that was not present on entry
        List<Object> contexts = Current.getContexts();

        while (contexts.size() > contextsOnEntry.size()) {
            Current.pop();
            contexts = Current.getContexts();
        }

        // the request phase may have removed an LRA that was present on entry (for example
        // NOT_SUPPORTED pops the whole stack); push it back so the enclosing context survives
        for (Object contextOnEntry : contextsOnEntry) {
            if (!contexts.contains(contextOnEntry)) {
                Current.push((URI) contextOnEntry, null);
                contexts = Current.getContexts();
            }
        }

        // restore the auth token found on entry (the request phase may have set one via @PropagateToken)
        if (authTokenOnEntry == null) {
            Current.clearAuthToken();
        } else {
            Current.setAuthToken(authTokenOnEntry);
        }
    }

    private Class<?> resolveResourceClass(InvocationContext invocationContext, Method method) {
        Class<?> declaringClass = method.getDeclaringClass();

        if (declaringClass.isInterface()) {
            Object target = invocationContext.getTarget();

            if (target != null) {
                return target.getClass();
            }
        }

        return declaringClass;
    }

    private ServerLRAProcessor getProcessor() {
        if (processor == null) {
            synchronized (this) {
                if (processor == null) {
                    processor = new ServerLRAProcessor(lraParticipantRegistry, data, enlistMaxRetries,
                            new CDILRAAnnotationResolver());
                }
            }
        }

        return processor;
    }
}
