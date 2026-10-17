/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client.internal.proxy.nonjaxrs;

import static io.narayana.lra.callbacks.CallbackType.AFTER;
import static io.narayana.lra.callbacks.CallbackType.COMPENSATE;
import static io.narayana.lra.callbacks.CallbackType.COMPLETE;
import static io.narayana.lra.callbacks.CallbackType.FORGET;
import static io.narayana.lra.callbacks.CallbackType.STATUS;

import io.narayana.lra.AnnotationResolver;
import io.narayana.lra.api.LRA;
import io.narayana.lra.callbacks.CallbackType;
import io.narayana.lra.callbacks.HttpCallback;
import io.narayana.lra.callbacks.ParticipantCallbacks;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.OPTIONS;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
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
 * Keeps references to individual non-JAX-RS participant methods in
 * single LRA participant class.
 */
public class LRAParticipant {

    private static final Class<? extends Annotation>[] HTTP_VERBS = new Class[] {
            GET.class, POST.class, PUT.class, DELETE.class, HEAD.class, OPTIONS.class
    };

    private final Class<?> javaClass;
    private final boolean transportNeutral; // whether this participant uses the io.narayana.lra.api annotations
    private final EnumMap<CallbackType, Method> callbackMethods = new EnumMap<>(CallbackType.class);
    private Object instance;

    private final Map<URI, ParticipantResult> participantStatusMap = new HashMap<>();

    public LRAParticipant(Class<?> javaClass) {
        this.javaClass = javaClass;
        transportNeutral = isTransportNeutral(javaClass);

        for (Method method : javaClass.getMethods()) {
            if (isJaxRsMethod(method)) {
                continue;
            }

            CallbackType type = callbackTypeOf(method);

            if (type == null) {
                continue;
            }

            verifySignature(method, type);
            callbackMethods.put(type, method);
        }
    }

    // a participant belongs to one world only: the beans managed by the CDI interceptor carry the
    // transport-neutral @LRA.Transaction annotation, everything else the MicroProfile @LRA one
    private static boolean isTransportNeutral(Class<?> participantClass) {
        return participantClass.isAnnotationPresent(LRA.Transaction.class)
                || Arrays.stream(participantClass.getMethods())
                        .anyMatch(method -> AnnotationResolver.isAnnotationPresent(LRA.Transaction.class, method));
    }

    Class<?> getJavaClass() {
        return javaClass;
    }

    synchronized Response compensate(URI lraId, URI parentId) {
        return dispatch(COMPENSATE, lraId, parentId);
    }

    synchronized Response complete(URI lraId, URI parentId) {
        return dispatch(COMPLETE, lraId, parentId);
    }

    synchronized Response status(URI lraId, URI parentId) {
        return dispatch(STATUS, lraId, parentId);
    }

    synchronized Response forget(URI lraId, URI parentId) {
        return dispatch(FORGET, lraId, parentId);
    }

    synchronized Response afterLRA(URI lraId, LRAStatus lraStatus) {
        Method method = callbackMethods.get(AFTER);
        Class<?>[] parameterTypes = method.getParameterTypes();
        Object[] args = new Object[parameterTypes.length];

        if (parameterTypes.length > 0) {
            args[0] = lraId;
        }

        if (parameterTypes.length > 1) {
            // the transport-neutral API uses LRA.TransactionStatus whereas the protocol uses LRAStatus
            args[1] = parameterTypes[1].equals(LRA.TransactionStatus.class) && lraStatus != null
                    ? LRA.TransactionStatus.valueOf(lraStatus.name())
                    : lraStatus;
        }

        Object result = invokeMethod(AFTER, method, getInstance(), args);

        return result instanceof Response ? (Response) result : Response.ok().build();
    }

    // the compensate/complete callbacks drain a pending asynchronous result first, status returns
    // it, forget has none
    private synchronized Response dispatch(CallbackType type, URI lraId, URI parentId) {
        if (type != FORGET && participantStatusMap.containsKey(lraId)) {
            Response staged = processCompletionStageResult(type, lraId, parentId);

            if (type == STATUS) {
                return staged;
            }

            staged.close();
        }

        return invokeParticipantMethod(type, lraId, parentId);
    }

    private CallbackType callbackTypeOf(Method method) {
        if (hasCallbackAnnotation(method, AfterLRA.class, LRA.AfterLRA.class)) {
            return AFTER;
        }
        if (hasCallbackAnnotation(method, Compensate.class, LRA.Compensate.class)) {
            return COMPENSATE;
        }
        if (hasCallbackAnnotation(method, Complete.class, LRA.Complete.class)) {
            return COMPLETE;
        }
        if (hasCallbackAnnotation(method, Status.class, LRA.Status.class)) {
            return STATUS;
        }
        if (hasCallbackAnnotation(method, Forget.class, LRA.Forget.class)) {
            return FORGET;
        }

        return null;
    }

    private boolean hasCallbackAnnotation(Method method, Class<? extends Annotation> microProfile,
            Class<? extends Annotation> transportNeutralAnnotation) {
        return AnnotationResolver.isAnnotationPresent(
                transportNeutral ? transportNeutralAnnotation : microProfile, method);
    }

    private void verifySignature(Method method, CallbackType type) {
        Class<?>[] parameterTypes = method.getParameterTypes();

        if (type == AFTER) {
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
            if (parameterTypes.length > 1 && !parameterTypes[1].equals(LRAStatus.class)
                    && !parameterTypes[1].equals(LRA.TransactionStatus.class)) {
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

    private boolean isJaxRsMethod(Method method) {
        return Arrays.stream(HTTP_VERBS)
                .anyMatch(annotationType -> AnnotationResolver.isAnnotationPresent(annotationType, method));
    }

    private void verifyReturnType(Method method) {
        Class<?> returnType = method.getReturnType();

        if (returnType.equals(CompletionStage.class)) {
            verifyReturnType(getCompletionStageActualType(method), method.toGenericString(), true);
            return;
        }

        verifyReturnType(returnType, method.toGenericString(), false);
    }

    private void verifyReturnType(Class<?> returnType, String methodGenericString, boolean inCompletionStage) {
        if (!returnType.equals(Void.TYPE) &&
                !returnType.equals(Void.class) &&
                !returnType.equals(ParticipantStatus.class) &&
                !returnType.equals(LRA.ParticipantOutcome.class) &&
                !returnType.equals(Response.class)) {
            throw new IllegalStateException(String.format("%s: %s",
                    methodGenericString, "Invalid return type for participant method "
                            + (inCompletionStage ? "CompletionStage<" + returnType + ">" : returnType)));
        }
    }

    private Response processCompletionStageResult(CallbackType type, URI lraId, URI parentId) {
        ParticipantResult participantResult = participantStatusMap.get(lraId);
        if (participantResult.isReady()) {
            participantStatusMap.remove(lraId);

            Object result = participantResult.getValue();

            if (shouldInvokeParticipantMethod(result)) {
                LRALogger.i18nLogger.warn_participantReturnsImmediateStateFromCompletionStage(
                        getJavaClass().getName(), lraId.toASCIIString());
                return invokeParticipantMethod(type, lraId, parentId);
            }

            return processResult(result, participantResult.getType(), type);
        } else {
            return Response.accepted().build(); // still compensating / completing
        }
    }

    private boolean shouldInvokeParticipantMethod(Object result) {
        if (result instanceof ParticipantStatus) {
            ParticipantStatus participantStatus = (ParticipantStatus) result;
            return participantStatus.equals(ParticipantStatus.Compensating) ||
                    participantStatus.equals(ParticipantStatus.Completing);
        } else if (result instanceof LRA.ParticipantOutcome) {
            LRA.ParticipantOutcome participantOutcome = (LRA.ParticipantOutcome) result;
            return participantOutcome.equals(LRA.ParticipantOutcome.Compensating) ||
                    participantOutcome.equals(LRA.ParticipantOutcome.Completing);
        } else {
            return result instanceof Response && ((Response) result).getStatus() == Response.Status.ACCEPTED.getStatusCode();
        }
    }

    private Response invokeParticipantMethod(CallbackType type, URI lraId, URI parentId) {
        Method method = callbackMethods.get(type);
        Object participant = getInstance();
        Object result;

        switch (method.getParameterCount()) {
            case 0:
                result = invokeMethod(type, method, participant);
                break;
            case 1:
                result = invokeMethod(type, method, participant, lraId);
                break;
            case 2:
                result = invokeMethod(type, method, participant, lraId, parentId);
                break;
            default:
                throw new IllegalStateException(
                        method.toGenericString() + ": invalid number of arguments: " + method.getParameterCount());
        }

        return processResult(result, lraId, type);
    }

    private Object getInstance() {
        return instance != null ? instance : CDI.current().select(javaClass).get();
    }

    private Object invokeMethod(CallbackType type, Method method, Object target, Object... args) {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            return processThrowable(e.getTargetException(), type);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private Response processResult(Object result, URI lraId, CallbackType type) {
        if (result instanceof CompletionStage) {
            // store the CompletionStage result and respond compensating / completing
            participantStatusMap.put(lraId, new ParticipantResult(getCompletionStageActualType(callbackMethods.get(type))));
            ((CompletionStage<?>) result)
                    .thenAccept(res -> participantStatusMap.get(lraId).setValue(res))
                    .exceptionally(throwable -> {
                        participantStatusMap.get(lraId).setValue(throwable);
                        return null;
                    });
            return Response.status(Response.Status.ACCEPTED).build();
        }

        return processResult(result, callbackMethods.get(type).getReturnType(), type);
    }

    private Response processResult(Object result, Class<?> resultType, CallbackType type) {
        Response.ResponseBuilder builder = Response.status(Response.Status.OK);

        if (resultType.equals(Void.TYPE) || resultType.equals(Void.class)) {
            builder.entity(type == COMPLETE ? ParticipantStatus.Completed.name() : ParticipantStatus.Compensated.name());
        } else if (result == null) {
            builder.status(Response.Status.NOT_FOUND);
        } else if (result instanceof ParticipantStatus) {
            ParticipantStatus status = (ParticipantStatus) result;
            if (status == ParticipantStatus.Compensating || status == ParticipantStatus.Completing) {
                return builder.status(Response.Status.ACCEPTED).build();
            } else {
                builder.entity(status.name());
            }
        } else if (result instanceof LRA.ParticipantOutcome) {
            LRA.ParticipantOutcome outcome = (LRA.ParticipantOutcome) result;
            if (outcome == LRA.ParticipantOutcome.Compensating || outcome == LRA.ParticipantOutcome.Completing) {
                return builder.status(Response.Status.ACCEPTED).build();
            } else {
                builder.entity(outcome.name());
            }
        } else if (result instanceof Response) {
            return (Response) result;
        } else {
            // the result must be a Throwable as no other option exists (signatures are checked during deployment)
            builder.entity(processThrowable((Throwable) result, type));
        }

        return builder.build();
    }

    private Object processThrowable(Throwable throwable, CallbackType type) {
        if (throwable instanceof WebApplicationException) {
            return ((WebApplicationException) throwable).getResponse();
        }

        if (type == COMPENSATE) {
            LRALogger.logger.debug("Compensate participant method threw an unexpected exception", throwable);
            return ParticipantStatus.FailedToCompensate.name();
        }

        if (type == COMPLETE) {
            LRALogger.logger.debug("Complete participant method threw an unexpected exception", throwable);
            return ParticipantStatus.FailedToComplete.name();
        }

        // @Status and @Forget should return HTTP 500
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }

    public void setInstance(Object instance) {
        this.instance = instance;
    }

    /**
     * The callbacks the coordinator should be given for this participant. A callback method that
     * declares a JAX-RS {@code @Path} endpoint is registered at it; every other callback has its
     * endpoint generated by the participant proxy (when {@code baseUri} is given).
     */
    public ParticipantCallbacks terminationCallbacks(String uriPrefix, URI baseUri) {
        ParticipantCallbacks callbacks = new ParticipantCallbacks();

        boolean asyncTermination = addPathCallbacks(callbacks, uriPrefix);
        addGeneratedCallbacks(callbacks, baseUri);
        checkAsyncTermination(callbacks, asyncTermination);

        return callbacks;
    }

    // registers the JAX-RS @Path endpoints of the class
    private boolean addPathCallbacks(ParticipantCallbacks callbacks, String uriPrefix) {
        boolean asyncTermination = false;

        for (Method method : javaClass.getMethods()) {
            Path pathAnnotation = method.getAnnotation(Path.class);

            if (pathAnnotation == null) {
                continue;
            }

            for (CallbackType type : CallbackType.values()) {
                if (!hasMicroProfileAnnotation(method, type)) {
                    continue;
                }

                addEndpoint(callbacks, method, pathAnnotation, type, uriPrefix);

                if (isAsyncCompletion(method) && (type == COMPENSATE || type == COMPLETE)) {
                    asyncTermination = true;
                }
            }
        }

        return asyncTermination;
    }

    private void addEndpoint(ParticipantCallbacks callbacks, Method method, Path pathAnnotation,
            CallbackType type, String uriPrefix) {
        if (hasHttpVerb(method)) {
            String pathValue = pathAnnotation.value();
            pathValue = pathValue.startsWith("/") ? pathValue : "/" + pathValue;
            callbacks.callback(type, HttpCallback.of(type, URI.create(uriPrefix + pathValue)));
        }
    }

    // every callback without an explicit @Path endpoint is served by the participant proxy
    private void addGeneratedCallbacks(ParticipantCallbacks callbacks, URI baseUri) {
        if (baseUri == null) {
            return;
        }

        String proxyPrefix = UriBuilder.fromUri(baseUri)
                .path(LRAParticipantResource.RESOURCE_PATH)
                .path(javaClass.getName())
                .build().toASCIIString();

        for (CallbackType type : CallbackType.values()) {
            if (callbacks.callback(type) == null && callbackMethods.containsKey(type)) {
                callbacks.callback(type, HttpCallback.of(type, URI.create(proxyPrefix + "/" + type.rel())));
            }
        }
    }

    // an asynchronous termination without a @Status or @Forget callback is invalid
    private void checkAsyncTermination(ParticipantCallbacks callbacks, boolean asyncTermination) {
        if (asyncTermination && callbacks.callback(STATUS) == null && callbacks.callback(FORGET) == null) {
            String logMsg = LRALogger.i18nLogger.error_asyncTerminationBeanMissStatusAndForget(javaClass);
            LRALogger.logger.warn(logMsg);
            throw new WebApplicationException(
                    Response.status(Response.Status.BAD_REQUEST).entity(logMsg).build());
        }
    }

    // the JAX-RS @Path endpoints carry only the MicroProfile annotations; the transport-neutral
    // LRA.* annotations apply to beans, never to @Path endpoints
    private boolean hasMicroProfileAnnotation(Method method, CallbackType type) {
        return switch (type) {
            case COMPENSATE -> method.isAnnotationPresent(Compensate.class);
            case COMPLETE -> method.isAnnotationPresent(Complete.class);
            case STATUS -> method.isAnnotationPresent(Status.class);
            case FORGET -> method.isAnnotationPresent(Forget.class);
            case AFTER -> method.isAnnotationPresent(AfterLRA.class);
        };
    }

    private static boolean hasHttpVerb(Method method) {
        return Arrays.stream(method.getDeclaredAnnotations()).anyMatch(annotation -> {
            String name = annotation.annotationType().getName();

            return name.equals(GET.class.getName()) ||
                    name.equals(PUT.class.getName()) ||
                    name.equals(POST.class.getName()) ||
                    name.equals(DELETE.class.getName());
        });
    }

    /**
     * @return whether the method is defined to be completed asynchronously, ie has a JAX-RS
     *         {@link Suspended} parameter while annotated with {@link Complete} or {@link Compensate}
     */
    public static boolean isAsyncCompletion(Method method) {
        if (method.isAnnotationPresent(Complete.class) || method.isAnnotationPresent(Compensate.class)) {
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

    boolean hasNonJaxRsMethods() {
        return !callbackMethods.isEmpty();
    }

    private static Class<?> getCompletionStageActualType(Method method) {
        Type genericReturnType = method.getGenericReturnType();
        ParameterizedType parameterizedType = (ParameterizedType) genericReturnType;
        return (Class<?>) parameterizedType.getActualTypeArguments()[0];
    }

    private static final class ParticipantResult {

        private boolean ready;
        private final Class<?> type;
        private Object value;

        ParticipantResult(Class<?> type) {
            this.type = type;
        }

        boolean isReady() {
            return ready;
        }

        void setValue(Object value) {
            this.value = value;
            this.ready = true;
        }

        Class<?> getType() {
            return type;
        }

        Object getValue() {
            return value;
        }
    }
}
