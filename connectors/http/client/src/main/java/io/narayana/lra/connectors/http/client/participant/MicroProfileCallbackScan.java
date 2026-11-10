/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.participant;

import io.narayana.lra.AnnotationResolver;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.contracts.ParticipantReply;
import io.narayana.lra.connectors.common.contracts.ParticipantRequest;
import io.narayana.lra.logging.LRALogger;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.OPTIONS;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Response;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.Forget;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;
import org.eclipse.microprofile.lra.annotation.Status;

/**
 * The JAX-RS read of the MicroProfile callback shape.
 * <p>
 * It discovers and validates the callback methods of one bean class: which methods are served by
 * the callback proxy, which declare their own endpoint, and whether a termination callback completes
 * asynchronously. Building the addresses from those facts is the connector's job, not the scan's.
 */
final class MicroProfileCallbackScan {

    private static final Class<? extends Annotation>[] HTTP_VERBS = new Class[] {
            GET.class, POST.class, PUT.class, DELETE.class, HEAD.class, OPTIONS.class
    };

    private final EnumMap<CallbackType, Method> generatedMethods = new EnumMap<>(CallbackType.class);
    private final EnumMap<CallbackType, String> ownEndpointPaths = new EnumMap<>(CallbackType.class);
    private boolean asyncTermination;

    private MicroProfileCallbackScan(Class<?> javaClass) {
        for (Method method : javaClass.getMethods()) {
            CallbackType type = callbackTypeOf(method);

            if (type == null) {
                continue;
            }

            if (isContractShape(method)) {
                // the method uses the transport-neutral callback contract; LRAParticipant owns it
                continue;
            }

            if (hasHttpVerb(method)) {
                Path path = AnnotationResolver.resolveAnnotation(Path.class, method);

                if (path != null) {
                    ownEndpointPaths.put(type, normalise(path.value()));

                    if (isAsyncCompletion(method) && (type == CallbackType.COMPENSATE || type == CallbackType.COMPLETE)) {
                        asyncTermination = true;
                    }
                }

                continue;
            }

            verifySignature(method, type);
            generatedMethods.put(type, method);
        }
    }

    static MicroProfileCallbackScan of(Class<?> javaClass) {
        return new MicroProfileCallbackScan(javaClass);
    }

    Map<CallbackType, Method> generatedMethods() {
        return generatedMethods;
    }

    Map<CallbackType, String> ownEndpointPaths() {
        return ownEndpointPaths;
    }

    boolean asyncTermination() {
        return asyncTermination;
    }

    static CallbackType callbackTypeOf(Method method) {
        if (AnnotationResolver.isAnnotationPresent(AfterLRA.class, method)) {
            return CallbackType.AFTER;
        }
        if (AnnotationResolver.isAnnotationPresent(Compensate.class, method)) {
            return CallbackType.COMPENSATE;
        }
        if (AnnotationResolver.isAnnotationPresent(Complete.class, method)) {
            return CallbackType.COMPLETE;
        }
        if (AnnotationResolver.isAnnotationPresent(Status.class, method)) {
            return CallbackType.STATUS;
        }
        if (AnnotationResolver.isAnnotationPresent(Forget.class, method)) {
            return CallbackType.FORGET;
        }

        return null;
    }

    static boolean hasHttpVerb(Method method) {
        return Arrays.stream(HTTP_VERBS)
                .anyMatch(annotationType -> AnnotationResolver.isAnnotationPresent(annotationType, method));
    }

    static String normalise(String path) {
        return path.startsWith("/") ? path : "/" + path;
    }

    static boolean isContractShape(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();

        if (parameterTypes.length == 1 && ParticipantRequest.class.isAssignableFrom(parameterTypes[0])) {
            return true;
        }

        Class<?> returnType = method.getReturnType();

        if (ParticipantReply.class.isAssignableFrom(returnType)) {
            return true;
        }

        if (returnType.equals(CompletionStage.class)) {
            Type genericReturnType = method.getGenericReturnType();

            return genericReturnType instanceof ParameterizedType parameterizedType
                    && parameterizedType.getActualTypeArguments()[0] instanceof Class<?> actual
                    && ParticipantReply.class.isAssignableFrom(actual);
        }

        return false;
    }

    private static void verifySignature(Method method, CallbackType type) {
        Class<?>[] parameterTypes = method.getParameterTypes();

        if (type == CallbackType.AFTER) {
            if (!method.getReturnType().equals(Void.TYPE)) {
                throw new IllegalStateException(method.toGenericString() + ": invalid return type for @AfterLRA method");
            }
            if (parameterTypes.length > 2) {
                throw new IllegalStateException(
                        method.toGenericString() + ": @AfterLRA method cannot have more than 2 arguments");
            }
            if (parameterTypes.length > 0 && !parameterTypes[0].equals(URI.class)) {
                throw new IllegalStateException(method.toGenericString() + ": invalid first argument of @AfterLRA method");
            }
            if (parameterTypes.length > 1 && !parameterTypes[1].equals(LRAStatus.class)) {
                throw new IllegalStateException(method.toGenericString() + ": invalid second argument of @AfterLRA method");
            }
            return;
        }

        verifyReturnType(method);

        if (parameterTypes.length > 2) {
            throw new IllegalStateException(
                    method.toGenericString() + ": participant method cannot have more than 2 arguments");
        }
        if (parameterTypes.length > 0 && !parameterTypes[0].equals(URI.class)) {
            throw new IllegalStateException(method.toGenericString() + ": invalid argument type in LRA participant method");
        }
        if (parameterTypes.length > 1 && !parameterTypes[1].equals(URI.class)) {
            throw new IllegalStateException(method.toGenericString() + ": invalid argument type in LRA participant method");
        }
    }

    private static void verifyReturnType(Method method) {
        Class<?> returnType = method.getReturnType();

        if (returnType.equals(CompletionStage.class)) {
            verifyReturnType(getCompletionStageActualType(method), method.toGenericString(), true);
            return;
        }

        verifyReturnType(returnType, method.toGenericString(), false);
    }

    private static void verifyReturnType(Class<?> returnType, String methodGenericString, boolean inCompletionStage) {
        if (!returnType.equals(Void.TYPE) &&
                !returnType.equals(Void.class) &&
                !returnType.equals(ParticipantStatus.class) &&
                !returnType.equals(Response.class)) {
            throw new IllegalStateException(String.format("%s: %s",
                    methodGenericString, "Invalid return type for participant method "
                            + (inCompletionStage ? "CompletionStage<" + returnType + ">" : returnType)));
        }
    }

    static Class<?> getCompletionStageActualType(Method method) {
        Type genericReturnType = method.getGenericReturnType();
        ParameterizedType parameterizedType = (ParameterizedType) genericReturnType;
        return (Class<?>) parameterizedType.getActualTypeArguments()[0];
    }

    /**
     * @return whether the method is defined to be completed asynchronously, ie has a JAX-RS
     *         {@link Suspended} parameter while annotated with {@link Complete} or {@link Compensate}
     */
    static boolean isAsyncCompletion(Method method) {
        if (AnnotationResolver.isAnnotationPresent(Complete.class, method)
                || AnnotationResolver.isAnnotationPresent(Compensate.class, method)) {
            for (Annotation[] ann : method.getParameterAnnotations()) {
                for (Annotation an : ann) {
                    if (Suspended.class.isAssignableFrom(an.annotationType())) {
                        LRALogger.logger.warn("JAX-RS @Suspended annotation is untested");
                        return true;
                    }
                }
            }
        }

        return false;
    }
}
