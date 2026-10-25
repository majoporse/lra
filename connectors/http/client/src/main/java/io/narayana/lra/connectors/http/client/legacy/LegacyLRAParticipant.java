/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.legacy;

import static io.narayana.lra.connectors.common.callbacks.CallbackType.AFTER;
import static io.narayana.lra.connectors.common.callbacks.CallbackType.COMPENSATE;
import static io.narayana.lra.connectors.common.callbacks.CallbackType.COMPLETE;
import static io.narayana.lra.connectors.common.callbacks.CallbackType.FORGET;
import static io.narayana.lra.connectors.common.callbacks.CallbackType.STATUS;

import io.narayana.lra.AnnotationResolver;
import io.narayana.lra.api.LRA;
import io.narayana.lra.connectors.common.callbacks.CallbackAddress;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.client.ParticipantCallbackAddressResolver;
import io.narayana.lra.connectors.common.client.ParticipantCallbackTarget;
import io.narayana.lra.connectors.common.client.participant.Participant;
import io.narayana.lra.connectors.common.contracts.AfterLRACallback;
import io.narayana.lra.connectors.common.contracts.CompensateCallback;
import io.narayana.lra.connectors.common.contracts.CompleteCallback;
import io.narayana.lra.connectors.common.contracts.ForgetCallback;
import io.narayana.lra.connectors.common.contracts.ParticipantReply;
import io.narayana.lra.connectors.common.contracts.ParticipantRequest;
import io.narayana.lra.connectors.common.contracts.StatusCallback;
import io.narayana.lra.connectors.http.address.LegacyHttpAddress;
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
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.Forget;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;
import org.eclipse.microprofile.lra.annotation.Status;

/**
 * The MicroProfile LRA participant form: a bean whose callback methods are JAX-RS resource methods
 * (or plain methods when they do not need an endpoint of their own).
 * <p>
 * Callback methods that declare {@code @Path} and an HTTP verb are the participant's own endpoints
 * and are registered as such. Every other callback method has no endpoint, so it is served by the
 * transport-neutral callback proxy and registered as a contract address.
 */
public class LegacyLRAParticipant implements Participant {

    private static final Class<? extends Annotation>[] HTTP_VERBS = new Class[] {
            GET.class, POST.class, PUT.class, DELETE.class, HEAD.class, OPTIONS.class
    };

    private final Class<?> javaClass;

    // callback methods without an endpoint of their own; they are invoked reflectively
    private final EnumMap<CallbackType, Method> callbackMethods = new EnumMap<>(CallbackType.class);

    // callback methods that declare their own JAX-RS endpoint, keyed by callback kind
    private final EnumMap<CallbackType, String> pathCallbacks = new EnumMap<>(CallbackType.class);

    private final Map<URI, ParticipantResult> participantStatusMap = new HashMap<>();

    private boolean asyncTermination;

    private Object instance;

    public LegacyLRAParticipant(Class<?> javaClass) {
        this.javaClass = javaClass;

        for (Method method : javaClass.getMethods()) {
            CallbackType type = callbackTypeOf(method);

            if (type == null) {
                continue;
            }

            if (isJaxRsMethod(method)) {
                Path path = AnnotationResolver.resolveAnnotation(Path.class, method);

                if (path != null) {
                    pathCallbacks.put(type, normalise(path.value()));

                    if (isAsyncCompletion(method) && (type == COMPENSATE || type == COMPLETE)) {
                        asyncTermination = true;
                    }
                }

                continue;
            }

            verifySignature(method, type);
            callbackMethods.put(type, method);
        }
    }

    @Override
    public String participantId() {
        return javaClass.getName();
    }

    @Override
    public boolean hasCallbacks() {
        return !callbackMethods.isEmpty() || !pathCallbacks.isEmpty();
    }

    /**
     * @return whether the participant has callbacks the proxy has to invoke reflectively
     */
    public boolean hasGeneratedCallbacks() {
        return !callbackMethods.isEmpty();
    }

    public Class<?> getJavaClass() {
        return javaClass;
    }

    @Override
    public void setInstance(Object instance) {
        this.instance = instance;
    }

    @Override
    public ParticipantCallbacks terminationCallbacks(ParticipantCallbackAddressResolver resolver, String uriPrefix,
            URI baseUri) {
        ParticipantCallbacks callbacks = new ParticipantCallbacks();

        for (Map.Entry<CallbackType, String> entry : pathCallbacks.entrySet()) {
            if (uriPrefix != null) {
                callbacks.callback(entry.getKey(),
                        address(resolver, entry.getKey(), URI.create(uriPrefix + entry.getValue())));
            }
        }

        addGeneratedCallbacks(resolver, callbacks, baseUri);
        checkAsyncTermination(callbacks, asyncTermination);

        return callbacks;
    }

    @Override
    public synchronized ParticipantReply invoke(CallbackType type, ParticipantRequest request) {
        Method method = callbackMethods.get(type);

        if (method == null) {
            return null;
        }

        if (type != FORGET && participantStatusMap.containsKey(request.lraId)) {
            ParticipantReply staged = stagedReply(type, request);

            if (type == STATUS) {
                return staged;
            }
        }

        return invokeParticipantMethod(type, method, request);
    }

    // the coordinator polls @Status for a pending asynchronous termination; compensate and complete
    // are retried and invoke the participant method again
    private ParticipantReply stagedReply(CallbackType type, ParticipantRequest request) {
        ParticipantResult pending = participantStatusMap.get(request.lraId);

        if (!pending.isReady()) {
            return progress(type);
        }

        participantStatusMap.remove(request.lraId);

        Object result = pending.getValue();

        if (shouldInvokeParticipantMethod(result)) {
            LRALogger.i18nLogger.warn_participantReturnsImmediateStateFromCompletionStage(
                    javaClass.getName(), request.lraId.toASCIIString());
            return invokeParticipantMethod(type, callbackMethods.get(type), request);
        }

        return replyFor(result, pending.getType(), type);
    }

    private ParticipantReply invokeParticipantMethod(CallbackType type, Method method, ParticipantRequest request) {
        Object result;

        try {
            result = method.invoke(getInstance(), arguments(type, method, request));
        } catch (InvocationTargetException e) {
            return fromThrowable(type, e.getTargetException());
        } catch (IllegalAccessException e) {
            return error(type, e.getMessage());
        }

        if (result instanceof CompletionStage<?> stage) {
            return pendingReply(type, method, request, stage);
        }

        return replyFor(result, method.getReturnType(), type);
    }

    // store the pending stage and report that the termination is in progress; the coordinator polls
    // @Status to drain it
    private ParticipantReply pendingReply(CallbackType type, Method method, ParticipantRequest request,
            CompletionStage<?> stage) {
        ParticipantResult pending = new ParticipantResult(getCompletionStageActualType(method));
        participantStatusMap.put(request.lraId, pending);

        stage.thenAccept(pending::setValue)
                .exceptionally(throwable -> {
                    pending.setValue(throwable);
                    return null;
                });

        return progress(type);
    }

    private static Object[] arguments(CallbackType type, Method method, ParticipantRequest request) {
        int count = method.getParameterCount();
        Object[] args = new Object[count];

        if (count > 0) {
            args[0] = request.lraId;
        }

        if (count > 1) {
            args[1] = type == AFTER ? endStatus(request) : request.parentId;
        }

        return args;
    }

    private static LRAStatus endStatus(ParticipantRequest request) {
        if (request instanceof AfterLRACallback.Request after && after.endStatus != null) {
            return LRAStatus.valueOf(after.endStatus.name());
        }

        return null;
    }

    private Object getInstance() {
        return instance != null ? instance : CDI.current().select(javaClass).get();
    }

    private ParticipantReply replyFor(Object result, Class<?> resultType, CallbackType type) {
        if (resultType.equals(Void.TYPE) || resultType.equals(Void.class)) {
            return definitive(type);
        }

        if (result == null) {
            return error(type, "participant callback returned null");
        }

        if (result instanceof ParticipantStatus status) {
            return outcome(type, status);
        }

        if (result instanceof Response response) {
            return replyFor(type, response);
        }

        if (result instanceof Throwable throwable) {
            return fromThrowable(type, throwable);
        }

        return error(type, "unsupported participant callback result " + result.getClass().getName());
    }

    private ParticipantReply replyFor(CallbackType type, Response response) {
        int status = response.getStatus();

        if (status == Response.Status.ACCEPTED.getStatusCode()) {
            return progress(type);
        }

        if (status == Response.Status.NOT_FOUND.getStatusCode()) {
            return error(type, "participant reports that the callback does not exist");
        }

        if (status >= 200 && status < 300) {
            return entityOutcome(type, response);
        }

        return error(type, "participant reports HTTP " + status);
    }

    private ParticipantReply entityOutcome(CallbackType type, Response response) {
        if (!response.hasEntity()) {
            return definitive(type);
        }

        Object entity = response.getEntity();

        if (entity instanceof ParticipantStatus status) {
            return outcome(type, status);
        }

        if (entity instanceof String text) {
            for (ParticipantStatus status : ParticipantStatus.values()) {
                if (status.name().equals(text.trim())) {
                    return outcome(type, status);
                }
            }
        }

        return definitive(type);
    }

    private ParticipantReply fromThrowable(CallbackType type, Throwable throwable) {
        if (throwable instanceof WebApplicationException webApplicationException) {
            return replyFor(type, webApplicationException.getResponse());
        }

        String message = message(throwable);

        return switch (type) {
            case COMPENSATE -> new CompensateCallback.Reply(LRA.ParticipantOutcome.FailedToCompensate, message);
            case COMPLETE -> new CompleteCallback.Reply(LRA.ParticipantOutcome.FailedToComplete, message);
            case STATUS -> new StatusCallback.Reply(null, message);
            case FORGET -> new ForgetCallback.Reply(message);
            case AFTER -> new AfterLRACallback.Reply(message);
        };
    }

    // a callback that is not declared or does not exist is an error the coordinator must see
    private static ParticipantReply error(CallbackType type, String message) {
        return switch (type) {
            case COMPENSATE -> new CompensateCallback.Reply(null, message);
            case COMPLETE -> new CompleteCallback.Reply(null, message);
            case STATUS -> new StatusCallback.Reply(null, message);
            case FORGET -> new ForgetCallback.Reply(message);
            case AFTER -> new AfterLRACallback.Reply(message);
        };
    }

    private static String message(Throwable throwable) {
        return throwable.getMessage() != null ? throwable.getMessage() : throwable.getClass().getName();
    }

    private ParticipantReply outcome(CallbackType type, ParticipantStatus status) {
        if (status == ParticipantStatus.Compensating || status == ParticipantStatus.Completing) {
            return progress(type);
        }

        return reply(type, LRA.ParticipantOutcome.valueOf(status.name()));
    }

    private static ParticipantReply reply(CallbackType type, LRA.ParticipantOutcome outcome) {
        return switch (type) {
            case COMPENSATE -> new CompensateCallback.Reply(outcome);
            case COMPLETE -> new CompleteCallback.Reply(outcome);
            case STATUS -> new StatusCallback.Reply(outcome);
            case FORGET -> new ForgetCallback.Reply();
            case AFTER -> new AfterLRACallback.Reply();
        };
    }

    private static ParticipantReply progress(CallbackType type) {
        return switch (type) {
            case COMPENSATE -> new CompensateCallback.Reply(LRA.ParticipantOutcome.Compensating);
            case COMPLETE -> new CompleteCallback.Reply(LRA.ParticipantOutcome.Completing);
            case STATUS -> new StatusCallback.Reply(LRA.ParticipantOutcome.Compensating);
            // the contract has no in-progress reply for forget and after; an ack is the honest answer
            case FORGET, AFTER -> definitive(type);
        };
    }

    private static ParticipantReply definitive(CallbackType type) {
        return switch (type) {
            case COMPENSATE -> new CompensateCallback.Reply(LRA.ParticipantOutcome.Compensated);
            case COMPLETE -> new CompleteCallback.Reply(LRA.ParticipantOutcome.Completed);
            case STATUS -> new StatusCallback.Reply(LRA.ParticipantOutcome.Compensated);
            case FORGET -> new ForgetCallback.Reply();
            case AFTER -> new AfterLRACallback.Reply();
        };
    }

    private static boolean shouldInvokeParticipantMethod(Object result) {
        if (result instanceof ParticipantStatus status) {
            return status == ParticipantStatus.Compensating || status == ParticipantStatus.Completing;
        }

        return result instanceof Response response
                && response.getStatus() == Response.Status.ACCEPTED.getStatusCode();
    }

    private static String normalise(String path) {
        return path.startsWith("/") ? path : "/" + path;
    }

    private CallbackAddress address(ParticipantCallbackAddressResolver resolver, CallbackType type, URI uri) {
        // no resolver is wired when the filter runs outside a CDI container (unit tests, plain JAX-RS)
        return resolver == null ? LegacyHttpAddress.of(type, uri, javaClass.getName())
                : resolver.address(type, javaClass.getName(), uri);
    }

    // every callback without an endpoint of its own is served by the transport-neutral callback proxy
    private void addGeneratedCallbacks(ParticipantCallbackAddressResolver resolver, ParticipantCallbacks callbacks,
            URI baseUri) {
        if (baseUri == null || resolver == null) {
            return;
        }

        Set<CallbackType> generated = new LinkedHashSet<>();

        for (CallbackType type : callbackMethods.keySet()) {
            if (callbacks.callback(type) == null) {
                generated.add(type);
            }
        }

        if (generated.isEmpty()) {
            return;
        }

        ParticipantCallbacks resolved = resolver.resolve(
                new ParticipantCallbackTarget(javaClass.getName(), generated, baseUri));

        generated.forEach(type -> callbacks.callback(type, resolved.callback(type)));
    }

    // an asynchronous termination without a @Status or @Forget callback is invalid
    private void checkAsyncTermination(ParticipantCallbacks callbacks, boolean async) {
        if (async && callbacks.callback(STATUS) == null && callbacks.callback(FORGET) == null) {
            String logMsg = LRALogger.i18nLogger.error_asyncTerminationBeanMissStatusAndForget(javaClass);
            LRALogger.logger.warn(logMsg);
            throw new WebApplicationException(
                    Response.status(Response.Status.BAD_REQUEST).entity(logMsg).build());
        }
    }

    private CallbackType callbackTypeOf(Method method) {
        if (AnnotationResolver.isAnnotationPresent(AfterLRA.class, method)) {
            return AFTER;
        }
        if (AnnotationResolver.isAnnotationPresent(Compensate.class, method)) {
            return COMPENSATE;
        }
        if (AnnotationResolver.isAnnotationPresent(Complete.class, method)) {
            return COMPLETE;
        }
        if (AnnotationResolver.isAnnotationPresent(Status.class, method)) {
            return STATUS;
        }
        if (AnnotationResolver.isAnnotationPresent(Forget.class, method)) {
            return FORGET;
        }

        return null;
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
                !returnType.equals(Response.class)) {
            throw new IllegalStateException(String.format("%s: %s",
                    methodGenericString, "Invalid return type for participant method "
                            + (inCompletionStage ? "CompletionStage<" + returnType + ">" : returnType)));
        }
    }

    private static Class<?> getCompletionStageActualType(Method method) {
        Type genericReturnType = method.getGenericReturnType();
        ParameterizedType parameterizedType = (ParameterizedType) genericReturnType;
        return (Class<?>) parameterizedType.getActualTypeArguments()[0];
    }

    /**
     * @return whether the method is defined to be completed asynchronously, ie has a JAX-RS
     *         {@link Suspended} parameter while annotated with {@link Complete} or {@link Compensate}
     */
    public static boolean isAsyncCompletion(Method method) {
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
