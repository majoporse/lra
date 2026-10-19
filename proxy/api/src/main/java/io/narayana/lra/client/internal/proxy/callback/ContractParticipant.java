package io.narayana.lra.client.internal.proxy.callback;

import io.narayana.lra.AnnotationResolver;
import io.narayana.lra.api.LRA;
import io.narayana.lra.callbacks.CallbackFactory;
import io.narayana.lra.callbacks.CallbackType;
import io.narayana.lra.callbacks.ParticipantCallbacks;
import io.narayana.lra.callbacks.contracts.common.AfterLRACallback;
import io.narayana.lra.callbacks.contracts.common.CompensateCallback;
import io.narayana.lra.callbacks.contracts.common.CompleteCallback;
import io.narayana.lra.callbacks.contracts.common.ForgetCallback;
import io.narayana.lra.callbacks.contracts.common.ParticipantReply;
import io.narayana.lra.callbacks.contracts.common.ParticipantRequest;
import io.narayana.lra.callbacks.contracts.common.StatusCallback;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.ws.rs.core.UriBuilder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public class ContractParticipant {

    static final String RESOURCE_PATH = "lra-callback-proxy";

    private static final Map<CallbackType, Class<?>> REQUEST_TYPES = Map.of(
            CallbackType.COMPENSATE, CompensateCallback.Request.class,
            CallbackType.COMPLETE, CompleteCallback.Request.class,
            CallbackType.STATUS, StatusCallback.Request.class,
            CallbackType.FORGET, ForgetCallback.Request.class,
            CallbackType.AFTER, AfterLRACallback.Request.class);

    private static final Map<CallbackType, Class<?>> REPLY_TYPES = Map.of(
            CallbackType.COMPENSATE, CompensateCallback.Reply.class,
            CallbackType.COMPLETE, CompleteCallback.Reply.class,
            CallbackType.STATUS, StatusCallback.Reply.class,
            CallbackType.FORGET, ForgetCallback.Reply.class,
            CallbackType.AFTER, AfterLRACallback.Reply.class);

    private final Class<?> javaClass;
    private final String participantId;
    private final EnumMap<CallbackType, Method> callbackMethods = new EnumMap<>(CallbackType.class);
    private final CallbackFactory callbackFactory;
    private final Map<URI, PendingResult> pending = new HashMap<>();

    private Object instance;

    public ContractParticipant(Class<?> javaClass) {
        this.javaClass = javaClass;
        this.participantId = javaClass.getName();
        this.callbackFactory = CallbackFactory.forContractParticipant(participantId);
        discoverCallbackMethods();
    }

    private void discoverCallbackMethods() {
        List<String> errors = new ArrayList<>();

        for (Method method : javaClass.getMethods()) {
            CallbackType type;

            try {
                type = callbackTypeOf(method);
            } catch (IllegalStateException e) {
                errors.add(e.getMessage());
                continue;
            }

            if (type == null) {
                continue;
            }

            String signatureError = verifySignature(method, type);

            if (signatureError != null) {
                errors.add(signatureError);
                continue;
            }

            Method previous = callbackMethods.putIfAbsent(type, method);

            if (previous != null) {
                errors.add(String.format("%s: @LRA.%s is declared more than once (%s and %s)",
                        javaClass.getName(), type, previous.toGenericString(), method.toGenericString()));
            }
        }

        requireParticipantCallback(errors);
        requireStatusOrForgetForAsyncTermination(errors);

        if (!errors.isEmpty()) {
            throw new IllegalStateException(String.join(System.lineSeparator(), errors));
        }
    }

    private void requireParticipantCallback(List<String> errors) {
        if (!callbackMethods.containsKey(CallbackType.COMPENSATE)
                && !callbackMethods.containsKey(CallbackType.COMPLETE)
                && !callbackMethods.containsKey(CallbackType.AFTER)) {
            errors.add(String.format(
                    "%s declares no @LRA.Compensate, @LRA.Complete or @LRA.AfterLRA callback",
                    javaClass.getName()));
        }
    }

    private void requireStatusOrForgetForAsyncTermination(List<String> errors) {
        if (callbackMethods.containsKey(CallbackType.STATUS) || callbackMethods.containsKey(CallbackType.FORGET)) {
            return;
        }

        for (CallbackType type : List.of(CallbackType.COMPENSATE, CallbackType.COMPLETE)) {
            Method method = callbackMethods.get(type);

            if (isAsynchronous(method)) {
                errors.add(String.format(
                        "%s: completes termination asynchronously but declares neither @LRA.Status nor @LRA.Forget",
                        method.toGenericString()));
            }
        }
    }

    private static boolean isAsynchronous(Method method) {
        return method != null && method.getReturnType().equals(CompletionStage.class);
    }

    private CallbackType callbackTypeOf(Method method) {
        CallbackType found = null;

        for (CallbackAnnotation callback : CallbackAnnotation.values()) {
            if (!AnnotationResolver.isAnnotationPresent(callback.annotation, method)) {
                continue;
            }

            if (found != null) {
                throw new IllegalStateException(String.format(
                        "%s: a callback method may declare only one LRA callback annotation",
                        method.toGenericString()));
            }

            found = callback.type;
        }

        return found;
    }

    private String verifySignature(Method method, CallbackType type) {
        Class<?> requestType = REQUEST_TYPES.get(type);
        Class<?> replyType = REPLY_TYPES.get(type);
        Class<?>[] parameterTypes = method.getParameterTypes();

        if (parameterTypes.length != 1 || !parameterTypes[0].equals(requestType)) {
            return String.format("%s: a @LRA.%s callback must take exactly one %s parameter",
                    method.toGenericString(), type, requestType.getName());
        }

        Class<?> returnType = method.getReturnType();

        if (returnType.equals(CompletionStage.class)) {
            if (type == CallbackType.FORGET || type == CallbackType.AFTER) {
                return String.format("%s: a @LRA.%s callback cannot complete asynchronously",
                        method.toGenericString(), type);
            }

            Type genericReturnType = method.getGenericReturnType();

            if (!(genericReturnType instanceof ParameterizedType parameterizedType)
                    || !parameterizedType.getActualTypeArguments()[0].equals(replyType)) {
                return String.format("%s: a @LRA.%s callback must return CompletionStage<%s>",
                        method.toGenericString(), type, replyType.getName());
            }
        } else if (!returnType.equals(replyType)) {
            return String.format("%s: a @LRA.%s callback must return %s or CompletionStage<%s>",
                    method.toGenericString(), type, replyType.getName(), replyType.getName());
        }

        return null;
    }

    public String participantId() {
        return participantId;
    }

    public boolean hasCallback(CallbackType type) {
        return callbackMethods.containsKey(type);
    }

    public ParticipantCallbacks terminationCallbacks(String uriPrefix, URI baseUri) {
        ParticipantCallbacks callbacks = new ParticipantCallbacks();

        if (baseUri == null && !callbackFactory.isKafka()) {
            return callbacks;
        }

        URI callbackBase = baseUri != null ? baseUri : URI.create("http://localhost");

        for (CallbackType type : callbackMethods.keySet()) {
            URI uri = UriBuilder.fromUri(callbackBase)
                    .path(RESOURCE_PATH)
                    .path(participantId)
                    .path(type.rel())
                    .build();

            callbacks.callback(type, callbackFactory.of(type, uri));
        }

        return callbacks;
    }

    public synchronized ParticipantReply invoke(CallbackType type, ParticipantRequest request) {
        Method method = callbackMethods.get(type);

        if (method == null) {
            return null;
        }

        if (type == CallbackType.COMPENSATE || type == CallbackType.COMPLETE || type == CallbackType.STATUS) {
            PendingResult pendingResult = pending.get(request.lraId);

            if (pendingResult != null) {
                if (!pendingResult.isDone()) {
                    return progressReply(type, pendingResult);
                }

                pending.remove(request.lraId);
                return pendingResult.reply(type);
            }
        }

        return invokeMethod(type, method, request);
    }

    private ParticipantReply invokeMethod(CallbackType type, Method method, ParticipantRequest request) {
        Object result;

        try {
            result = method.invoke(getInstance(), request);
        } catch (InvocationTargetException e) {
            return errorReply(type, e.getTargetException());
        } catch (ReflectiveOperationException | IllegalArgumentException e) {
            return errorReply(type, e);
        }

        if (result instanceof CompletionStage<?> stage) {
            PendingResult pendingResult = new PendingResult(type);
            pending.put(request.lraId, pendingResult);
            stage.whenComplete((value, failure) -> {
                synchronized (ContractParticipant.this) {
                    PendingResult current = pending.get(request.lraId);

                    if (current == pendingResult) {
                        pendingResult.complete(value, failure);
                    }
                }
            });

            return progressReply(type, pendingResult);
        }

        if (result == null) {
            return errorReply(type, new IllegalStateException(
                    method.toGenericString() + " returned null instead of a callback reply"));
        }

        return (ParticipantReply) result;
    }

    private Object getInstance() {
        return instance != null ? instance : CDI.current().select(javaClass).get();
    }

    public void setInstance(Object instance) {
        this.instance = instance;
    }

    public Class<?> getJavaClass() {
        return javaClass;
    }

    private static ParticipantReply progressReply(CallbackType requestType, PendingResult pendingResult) {
        LRA.ParticipantOutcome outcome = pendingResult.operation == CallbackType.COMPLETE
                ? LRA.ParticipantOutcome.Completing
                : LRA.ParticipantOutcome.Compensating;

        return switch (requestType) {
            case COMPENSATE -> new CompensateCallback.Reply(outcome);
            case COMPLETE -> new CompleteCallback.Reply(outcome);
            case STATUS -> new StatusCallback.Reply(outcome);
            case FORGET, AFTER -> null;
        };
    }

    private static ParticipantReply errorReply(CallbackType type, Throwable throwable) {
        String message = message(throwable);

        LRALogger.logger.debugf("Transport-neutral %s callback failed: %s", type, message);

        return switch (type) {
            case COMPENSATE -> new CompensateCallback.Reply(LRA.ParticipantOutcome.FailedToCompensate, message);
            case COMPLETE -> new CompleteCallback.Reply(LRA.ParticipantOutcome.FailedToComplete, message);
            case STATUS -> new StatusCallback.Reply(null, message);
            case FORGET -> new ForgetCallback.Reply(message);
            case AFTER -> new AfterLRACallback.Reply(message);
        };
    }

    private static String message(Throwable throwable) {
        if (throwable == null) {
            return "unknown callback failure";
        }

        String message = throwable.getMessage();
        return message == null || message.isBlank() ? throwable.getClass().getName() : message;
    }

    private enum CallbackAnnotation {
        COMPENSATE(LRA.Compensate.class, CallbackType.COMPENSATE),
        COMPLETE(LRA.Complete.class, CallbackType.COMPLETE),
        STATUS(LRA.Status.class, CallbackType.STATUS),
        FORGET(LRA.Forget.class, CallbackType.FORGET),
        AFTER(LRA.AfterLRA.class, CallbackType.AFTER);

        private final Class<? extends java.lang.annotation.Annotation> annotation;
        private final CallbackType type;

        CallbackAnnotation(Class<? extends java.lang.annotation.Annotation> annotation, CallbackType type) {
            this.annotation = annotation;
            this.type = type;
        }
    }

    private static final class PendingResult {
        private final CallbackType operation;
        private boolean done;
        private Object value;
        private Throwable failure;

        PendingResult(CallbackType operation) {
            this.operation = operation;
        }

        boolean isDone() {
            return done;
        }

        void complete(Object value, Throwable failure) {
            this.value = value;
            this.failure = failure;
            this.done = true;
        }

        ParticipantReply reply(CallbackType type) {
            if (failure != null) {
                return errorReply(type, failure);
            }

            if (value == null) {
                return errorReply(type, new IllegalStateException("asynchronous callback completed without a reply"));
            }

            return (ParticipantReply) value;
        }
    }
}
