/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.filter.interceptor;

import static io.narayana.lra.LRAConstants.ENLIST_PARTICIPANT_CLIENT_MAX_RETRY;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_PARENT_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

import io.narayana.lra.AnnotationResolver;
import io.narayana.lra.Current;
import io.narayana.lra.LRAConstants;
import io.narayana.lra.PropagateToken;
import io.narayana.lra.api.LifecycleException;
import io.narayana.lra.api.Outcome;
import io.narayana.lra.api.Result;
import io.narayana.lra.connectors.common.client.LRAAbortException;
import io.narayana.lra.connectors.common.client.coordinator.LRAClient;
import io.narayana.lra.connectors.common.client.interceptor.LRAAnnotationResolver;
import io.narayana.lra.connectors.common.client.interceptor.LRATransportContext;
import io.narayana.lra.connectors.common.client.invocation.LRAParticipantData;
import io.narayana.lra.connectors.common.client.invocation.LraInvocation;
import io.narayana.lra.connectors.common.client.invocation.ServerLRAContext;
import io.narayana.lra.connectors.common.client.participant.LRAParticipantRegistry;
import io.narayana.lra.connectors.common.client.participant.ParticipantCallbackAddressResolver;
import io.narayana.lra.connectors.common.client.participant.protocol.ParticipantProtocolRegistry;
import io.narayana.lra.connectors.http.client.jaxrs.LraInvocationState;
import io.narayana.lra.connectors.http.client.lifecycle.MicroProfileLRAAnnotationResolver;
import io.narayana.lra.connectors.http.client.lifecycle.ServerLRAProcessor;
import io.narayana.lra.logging.LRALogger;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.Interceptor;
import jakarta.interceptor.InvocationContext;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.OPTIONS;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;

/**
 * CDI interceptor that manages the LRA lifecycle of an annotated method. It is the only driver of
 * the lifecycle: it reads {@code @LRA}, {@code @Leave} and {@code @PropagateToken}, starts or joins
 * the LRA before the method runs and hands the final decision to the transport when the transport
 * owns it.
 * <p>
 * For a JAX-RS resource method the cancel rules are expressed as HTTP status codes, which only
 * exist after the method has returned. The interceptor therefore stores its state on the request
 * and a thin JAX-RS response filter completes the lifecycle with the final status. For a plain CDI
 * bean the interceptor completes the lifecycle itself, using the {@link Outcome} carried by a
 * returned {@link Result} (or the exception the method threw).
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

    private static final LRAAnnotationResolver RESOLVER = new MicroProfileLRAAnnotationResolver();

    private static final Class<? extends Annotation>[] HTTP_VERBS = new Class[] {
            GET.class, POST.class, PUT.class, DELETE.class, HEAD.class, OPTIONS.class
    };

    @Inject
    private Instance<LRAParticipantRegistry> lraParticipantRegistry;

    @Inject
    private ParticipantProtocolRegistry protocolRegistry;

    @Inject
    LRAParticipantData data;

    @Inject
    LRATransportContext transportContext;

    @Inject
    Instance<LraInvocationState> invocationState;

    @Inject
    @ConfigProperty(name = ENLIST_PARTICIPANT_CLIENT_MAX_RETRY, defaultValue = "3")
    int enlistMaxRetries;

    private volatile ServerLRAProcessor processor;

    @AroundInvoke
    public Object intercept(InvocationContext invocationContext) throws Exception {
        Method method = invocationContext.getMethod();
        boolean httpResource = isHttpResource(invocationContext, method);

        if (RESOLVER.isParticipantCallback(method) || (!actsOn(method) && !httpResource)) {
            return invocationContext.proceed();
        }

        if (!httpResource && isAsynchronous(method)) {
            throw new IllegalStateException(String.format(
                    "%s: an @LRA method on a plain CDI bean must not return CompletionStage; "
                            + "asynchronous LRAs are only supported for JAX-RS resource methods",
                    method.toGenericString()));
        }

        // capture the LRA context and auth token that were in effect before this invocation
        // so that the cleanup can restore them (this invocation must not clobber the context
        // of any enclosing LRA processing)
        List<Object> contextsOnEntry = Current.getContexts();
        String authTokenOnEntry = Current.getAuthToken();

        Class<?> resourceClass = resolveResourceClass(invocationContext, method);

        ServerLRAContext context = new ServerLRAContext(method, resourceClass);
        boolean deferred = httpResource && deferToTransport(context, contextsOnEntry, authTokenOnEntry);

        if (populateContext(context)) {
            getProcessor().processRequest(context);
        }

        updateRequestHeaders(context);

        if (httpResource) {
            patchHeaderParameters(invocationContext, method, context);
        }

        boolean aborted = context.isAborted();
        Object result = null;
        Throwable thrown = null;
        Outcome outcome = aborted ? Outcome.SERVER_ERROR : Outcome.SUCCESS;

        if (!aborted) {
            try {
                result = invocationContext.proceed();
                outcome = outcomeOf(result);
            } catch (Exception | Error e) {
                thrown = e;
                outcome = outcome.SERVER_ERROR;
            }
        }

        if (deferred) {
            // the response filter completes the lifecycle with the final HTTP status and restores
            // the thread-local state
            if (aborted) {
                throw createAbortException(context);
            }

            rethrow(thrown);
            return result;
        }

        try {
            getProcessor().processResponse(context, isCancel(method, outcome, thrown != null || aborted));
        } finally {
            restoreCurrent(context, contextsOnEntry, authTokenOnEntry);
        }

        if (aborted) {
            // the request processing stopped the invocation before the method could run
            throw createAbortException(context);
        }

        /*
         * report any failed steps (ie if the LRA lifecycle completed with failures) to the
         * caller. The HTTP adapter writes the failure message into the response entity; a CDI
         * invocation has no response, so the interceptor logs the failure and throws it, or
         * attaches it to an exception the method itself threw.
         */
        String failureMessage = getProcessor().processFailures(context);

        if (failureMessage != null) {
            LRALogger.logger.error(failureMessage);

            if (thrown != null) {
                thrown.addSuppressed(new LifecycleException(failureMessage));
            } else {
                throw new LifecycleException(failureMessage);
            }
        }

        rethrow(thrown);
        return result;
    }

    // a class-level or method-level @LRA, a @Leave or a @PropagateToken is what the interceptor acts on
    private static boolean actsOn(Method method) {
        return AnnotationResolver.isAnnotationPresent(LRA.class, method)
                || method.getDeclaringClass().getDeclaredAnnotation(LRA.class) != null
                || RESOLVER.isLeave(method)
                || propagateToken(method);
    }

    private static boolean propagateToken(Method method) {
        return AnnotationResolver.isAnnotationPresent(PropagateToken.class, method)
                || method.getDeclaringClass().isAnnotationPresent(PropagateToken.class);
    }

    // a JAX-RS resource method is completed by the response filter, not by the interceptor
    private boolean deferToTransport(ServerLRAContext context, List<Object> contextsOnEntry,
            String authTokenOnEntry) {
        LraInvocationState state = requestState();

        if (state == null || state.getRequestContext() == null) {
            return false;
        }

        state.getRequestContext().setProperty(LraInvocation.PROPERTY,
                new LraInvocation(context, contextsOnEntry, authTokenOnEntry));

        return true;
    }

    private LraInvocationState requestState() {
        try {
            return invocationState != null && invocationState.isResolvable() ? invocationState.get() : null;
        } catch (ContextNotActiveException e) {
            // a plain CDI invocation outside a request
            return null;
        }
    }

    private static boolean isHttpResource(InvocationContext invocationContext, Method method) {
        Object target = invocationContext.getTarget();
        Class<?> resourceClass = target != null ? target.getClass() : method.getDeclaringClass();

        if (resourceClass.isAnnotationPresent(Path.class)) {
            return true;
        }

        return Arrays.stream(HTTP_VERBS)
                .anyMatch(annotationType -> AnnotationResolver.isAnnotationPresent(annotationType, method));
    }

    private static boolean isAsynchronous(Method method) {
        return method.getReturnType().equals(CompletionStage.class);
    }

    // JAX-RS resolves @HeaderParam before the interceptor chain runs, so the LRA the interceptor
    // started or joined would otherwise not be visible to the business method. Replace the resolved
    // LRA header arguments (and only those) with the effective values.
    private static void patchHeaderParameters(InvocationContext invocationContext, Method method,
            ServerLRAContext context) {
        Object[] parameters = invocationContext.getParameters();

        if (parameters == null || parameters.length == 0) {
            return;
        }

        Class<?>[] parameterTypes = method.getParameterTypes();
        boolean changed = false;

        for (int i = 0; i < parameters.length; i++) {
            HeaderParam header = headerParam(method, i);

            if (header == null) {
                continue;
            }

            Object value;

            switch (header.value()) {
                case LRA_HTTP_CONTEXT_HEADER -> value = context.isClearContextHeader()
                        ? null
                        : context.getCurrentLRA() != null ? context.getCurrentLRA() : context.getIncomingLRA();
                case LRA_HTTP_PARENT_CONTEXT_HEADER -> value = context.getParentLRA();
                case LRA_HTTP_RECOVERY_HEADER -> value = context.getRecoveryUrl();
                default -> {
                    continue;
                }
            }

            if (value == null) {
                parameters[i] = null;
            } else if (parameterTypes[i].equals(String.class)) {
                parameters[i] = value.toString();
            } else if (parameterTypes[i].equals(URI.class)) {
                parameters[i] = value instanceof URI ? value : URI.create(value.toString());
            } else {
                parameters[i] = value;
            }
            changed = true;
        }

        if (changed) {
            invocationContext.setParameters(parameters);
        }
    }

    // a JAX-RS resource reads the LRA with @HeaderParam, so the LRA started or joined by the
    // interceptor must be published on the request before the business method runs
    private void updateRequestHeaders(ServerLRAContext context) {
        LraInvocationState state = requestState();

        if (state == null || state.getRequestContext() == null) {
            return;
        }

        MultivaluedMap<String, String> headers = state.getRequestContext().getHeaders();

        if (context.isParentHeaderHandled()) {
            if (context.getParentLRA() != null) {
                headers.putSingle(LRA_HTTP_PARENT_CONTEXT_HEADER, context.getParentLRA().toASCIIString());
            } else {
                headers.remove(LRA_HTTP_PARENT_CONTEXT_HEADER);
            }
        }

        if (context.isClearContextHeader()) {
            headers.remove(LRA_HTTP_CONTEXT_HEADER);
        } else if (context.getCurrentLRA() != null) {
            headers.putSingle(LRA_HTTP_CONTEXT_HEADER, context.getCurrentLRA().toString());
        }

        if (context.getRecoveryUrl() != null) {
            headers.putSingle(LRA_HTTP_RECOVERY_HEADER, context.getRecoveryUrl());
        }
    }

    // translate the transport state of the invocation into the context handed to the processor
    private boolean populateContext(ServerLRAContext context) {
        LraInvocationState state = requestState();
        ContainerRequestContext requestContext = state == null ? null : state.getRequestContext();

        String authHeader = transportContext.getHeader(HttpHeaders.AUTHORIZATION);

        if (authHeader == null && requestContext != null) {
            authHeader = requestContext.getHeaderString(HttpHeaders.AUTHORIZATION);
        }

        context.setAuthHeader(authHeader);

        List<String> headers = transportContext.getHeaders(LRA_HTTP_CONTEXT_HEADER);

        if ((headers == null || headers.isEmpty()) && requestContext != null) {
            String captured = requestContext.getHeaderString(LRA_HTTP_CONTEXT_HEADER);

            if (captured != null) {
                headers = List.of(captured);
            }
        }

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

        URI baseUri = requestContext != null
                ? requestContext.getUriInfo().getBaseUri()
                : (state != null && state.getBaseUri() != null ? state.getBaseUri() : transportContext.getBaseUri());
        context.setUriPrefix(() -> createUriPrefix(context.getResourceClass(), baseUri));
        context.setBaseUri(baseUri);

        return true;
    }

    // cancelling a plain CDI invocation has no HTTP status to match, so the cancel families are
    // mapped onto the transport-neutral Outcome carried by Result (or implied by an exception)
    private boolean isCancel(Method method, Outcome outcome, boolean failed) {
        LRA lra = AnnotationResolver.resolveAnnotation(LRA.class, method);

        if (lra == null) {
            lra = method.getDeclaringClass().getDeclaredAnnotation(LRA.class);
        }

        if (lra == null) {
            return failed;
        }

        if (lra.cancelOn().length > 0) {
            throw new IllegalStateException(String.format(
                    "%s: @LRA.cancelOn is not supported on a plain CDI bean method because there is no HTTP "
                            + "status; use cancelOnFamily and return io.narayana.lra.api.Result instead",
                    method.toGenericString()));
        }

        for (Response.Status.Family family : lra.cancelOnFamily()) {
            if (family == Response.Status.Family.CLIENT_ERROR && outcome == Outcome.CLIENT_ERROR) {
                return true;
            }

            if (family == Response.Status.Family.SERVER_ERROR && outcome == Outcome.SERVER_ERROR) {
                return true;
            }
        }

        return false;
    }

    private Outcome outcomeOf(Object result) {
        if (result instanceof Result<?> wrapper) {
            Outcome outcome = wrapper.getOutcome();
            return outcome == null ? Outcome.SUCCESS : outcome;
        }

        return Outcome.SUCCESS;
    }

    // mirrors the base uri resolution performed by the JAX-RS request adapter
    private String createUriPrefix(Class<?> resourceClass, URI requestBaseUri) {
        String base = LRAConstants.getBaseUri().orElseGet(
                () -> requestBaseUri == null ? null : requestBaseUri.toASCIIString());

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

    // restore the thread-local state that was in effect before this invocation
    private void restoreCurrent(ServerLRAContext context, List<Object> contextsOnEntry, String authTokenOnEntry) {
        new LraInvocation(context, contextsOnEntry, authTokenOnEntry).restoreCurrent();
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

    // JAX-RS parameter annotations may live on the interface or superclass method that the class
    // overrides, so look them up in the hierarchy as well
    private static HeaderParam headerParam(Method method, int index) {
        HeaderParam header = findHeaderParam(method.getParameterAnnotations()[index]);

        if (header != null) {
            return header;
        }

        for (Class<?> type : hierarchy(method.getDeclaringClass())) {
            try {
                Method candidate = type.getMethod(method.getName(), method.getParameterTypes());

                if (index < candidate.getParameterCount()) {
                    header = findHeaderParam(candidate.getParameterAnnotations()[index]);

                    if (header != null) {
                        return header;
                    }
                }
            } catch (NoSuchMethodException ignored) {
                // not declared by this type
            }
        }

        return null;
    }

    private static HeaderParam findHeaderParam(Annotation[] annotations) {
        for (Annotation annotation : annotations) {
            if (annotation instanceof HeaderParam header) {
                return header;
            }
        }

        return null;
    }

    private static java.util.List<Class<?>> hierarchy(Class<?> type) {
        java.util.List<Class<?>> types = new java.util.ArrayList<>();
        java.util.Deque<Class<?>> pending = new java.util.ArrayDeque<>();
        Collections.addAll(pending, type.getInterfaces());

        for (Class<?> superClass = type.getSuperclass(); superClass != null; superClass = superClass.getSuperclass()) {
            pending.add(superClass);
        }

        while (!pending.isEmpty()) {
            Class<?> current = pending.removeFirst();

            if (types.contains(current)) {
                continue;
            }

            types.add(current);
            Collections.addAll(pending, current.getInterfaces());

            for (Class<?> superClass = current.getSuperclass(); superClass != null; superClass = superClass.getSuperclass()) {
                pending.add(superClass);
            }
        }

        return types;
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

    private ServerLRAProcessor getProcessor() {
        if (processor == null) {
            synchronized (this) {
                if (processor == null) {
                    LRAParticipantRegistry registry = lraParticipantRegistry != null
                            && lraParticipantRegistry.isResolvable()
                                    ? lraParticipantRegistry.get()
                                    : null;
                    processor = new ServerLRAProcessor(registry, data, enlistMaxRetries,
                            RESOLVER, lraClient(), callbackResolver());
                }
            }
        }

        return processor;
    }

    private LRAClient lraClient() {
        ParticipantProtocolRegistry registry = protocolRegistry != null
                ? protocolRegistry
                : new ParticipantProtocolRegistry();

        return registry.client();
    }

    private ParticipantCallbackAddressResolver callbackResolver() {
        ParticipantProtocolRegistry registry = protocolRegistry != null
                ? protocolRegistry
                : new ParticipantProtocolRegistry();

        return registry.resolver();
    }
}
