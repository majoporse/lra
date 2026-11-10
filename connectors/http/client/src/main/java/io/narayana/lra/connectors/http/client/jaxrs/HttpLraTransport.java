/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.jaxrs;

import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_PARENT_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

import io.narayana.lra.AnnotationResolver;
import io.narayana.lra.Current;
import io.narayana.lra.connectors.common.client.LRAAbortException;
import io.narayana.lra.connectors.common.client.interceptor.LRATransport;
import io.narayana.lra.connectors.common.client.invocation.LraInvocation;
import io.narayana.lra.connectors.http.support.BearerTokenResolver;
import io.narayana.lra.logging.LRALogger;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
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
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The JAX-RS half of an intercepted invocation.
 * <p>
 * It reads the inbound LRA and credential from the request (falling back to the effective LRA on the
 * calling thread), publishes the effective LRA back onto the request and into any
 * {@code @HeaderParam} arguments JAX-RS already resolved, and claims the response phase for resource
 * methods, whose cancel rules are HTTP status codes.
 */
@ApplicationScoped
@Alternative
@Priority(1)
public class HttpLraTransport implements LRATransport {

    private static final Class<? extends Annotation>[] HTTP_VERBS = new Class[] {
            GET.class, POST.class, PUT.class, DELETE.class, HEAD.class, OPTIONS.class
    };

    private static final int PRECONDITION_FAILED = 412;
    private static final String BEARER_PREFIX = "Bearer ";

    @Inject
    Instance<HttpHeaders> httpHeaders;

    @Inject
    Instance<LraInvocationState> invocationState;

    @Override
    public URI incomingLRA() {
        URI effective = Current.peek();

        if (effective != null) {
            return effective;
        }

        String header = requestHeader(LRA_HTTP_CONTEXT_HEADER);

        if (header == null) {
            return null;
        }

        try {
            return URI.create(header);
        } catch (IllegalArgumentException e) {
            String msg = String.format("header %s contains an invalid URL %s", LRA_HTTP_CONTEXT_HEADER, header);
            LRALogger.i18nLogger.warn_lraFilterContainerRequest(msg, "<unknown>", "context");

            throw new LRAAbortException(PRECONDITION_FAILED, msg);
        }
    }

    @Override
    public String credential() {
        String authorization = requestHeader(HttpHeaders.AUTHORIZATION);

        if (authorization == null || !authorization.regionMatches(true, 0, BEARER_PREFIX, 0,
                BEARER_PREFIX.length())) {
            return null;
        }

        String token = authorization.substring(BEARER_PREFIX.length()).trim();

        if (BearerTokenResolver.isPlausibleJwt(token)) {
            return token;
        }

        LRALogger.logger.warnf("@PropagateToken: Authorization header does not contain a valid JWT structure");

        return null;
    }

    @Override
    public void bind(InvocationContext invocation, LraInvocation lraInvocation) {
        updateRequestHeaders(lraInvocation);
        patchHeaderParameters(invocation, lraInvocation);
    }

    @Override
    public boolean deferCompletion(InvocationContext invocation, LraInvocation lraInvocation) {
        if (!isHttpResource(invocation)) {
            return false;
        }

        LraInvocationState state = state();

        if (state == null || state.getRequestContext() == null) {
            return false;
        }

        state.getRequestContext().setProperty(LraInvocation.PROPERTY, lraInvocation);

        return true;
    }

    private LraInvocationState state() {
        try {
            return invocationState != null && invocationState.isResolvable() ? invocationState.get() : null;
        } catch (ContextNotActiveException e) {
            // a plain CDI invocation outside a request
            return null;
        }
    }

    private String requestHeader(String name) {
        ContainerRequestContext requestContext = requestContext();

        if (requestContext != null) {
            String value = requestContext.getHeaderString(name);

            if (value != null) {
                return value;
            }
        }

        try {
            if (httpHeaders != null && httpHeaders.isResolvable()) {
                List<String> values = httpHeaders.get().getRequestHeader(name);

                return Current.getLast(values);
            }
        } catch (ContextNotActiveException | IllegalStateException e) {
            // no request in progress
        }

        return null;
    }

    private ContainerRequestContext requestContext() {
        LraInvocationState state = state();

        return state == null ? null : state.getRequestContext();
    }

    private static boolean isHttpResource(InvocationContext invocationContext) {
        Method method = invocationContext.getMethod();
        Object target = invocationContext.getTarget();
        Class<?> resourceClass = target != null ? target.getClass() : method.getDeclaringClass();

        if (resourceClass.isAnnotationPresent(Path.class)) {
            return true;
        }

        return Arrays.stream(HTTP_VERBS)
                .anyMatch(annotationType -> AnnotationResolver.isAnnotationPresent(annotationType, method));
    }

    // JAX-RS resolves @HeaderParam before the interceptor chain runs, so the LRA the processor
    // started or joined would otherwise not be visible to the business method. Replace the resolved
    // LRA header arguments (and only those) with the effective values.
    private static void patchHeaderParameters(InvocationContext invocationContext, LraInvocation lraInvocation) {
        Object[] parameters = invocationContext.getParameters();

        if (parameters == null || parameters.length == 0) {
            return;
        }

        Method method = invocationContext.getMethod();
        Class<?>[] parameterTypes = method.getParameterTypes();
        boolean changed = false;

        for (int i = 0; i < parameters.length; i++) {
            HeaderParam header = headerParam(method, i);

            if (header == null) {
                continue;
            }

            Object value;

            switch (header.value()) {
                case LRA_HTTP_CONTEXT_HEADER -> value = lraInvocation.isClearIncomingLRA()
                        ? null
                        : lraInvocation.getCurrentLRA() != null ? lraInvocation.getCurrentLRA()
                                : lraInvocation.getIncomingLRA();
                case LRA_HTTP_PARENT_CONTEXT_HEADER -> value = lraInvocation.getParentLRA();
                case LRA_HTTP_RECOVERY_HEADER -> value = lraInvocation.getRecoveryUrl();
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

    // the request is the inbound carrier, so the LRA the processor started or joined must be
    // published on it before the business method runs (for readers of HttpHeaders and of the
    // unannotated-resource propagation rule)
    private void updateRequestHeaders(LraInvocation lraInvocation) {
        ContainerRequestContext requestContext = requestContext();

        if (requestContext == null) {
            return;
        }

        MultivaluedMap<String, String> headers = requestContext.getHeaders();

        if (lraInvocation.isParentDecided()) {
            if (lraInvocation.getParentLRA() != null) {
                headers.putSingle(LRA_HTTP_PARENT_CONTEXT_HEADER, lraInvocation.getParentLRA().toASCIIString());
            } else {
                headers.remove(LRA_HTTP_PARENT_CONTEXT_HEADER);
            }
        }

        if (lraInvocation.isClearIncomingLRA()) {
            headers.remove(LRA_HTTP_CONTEXT_HEADER);
        } else if (lraInvocation.getCurrentLRA() != null) {
            headers.putSingle(LRA_HTTP_CONTEXT_HEADER, lraInvocation.getCurrentLRA().toString());
        }

        if (lraInvocation.getRecoveryUrl() != null) {
            headers.putSingle(LRA_HTTP_RECOVERY_HEADER, lraInvocation.getRecoveryUrl());
        }
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

    private static List<Class<?>> hierarchy(Class<?> type) {
        List<Class<?>> types = new java.util.ArrayList<>();
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
}
