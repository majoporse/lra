/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.participant;

import static io.narayana.lra.connectors.common.callbacks.CallbackType.AFTER;
import static io.narayana.lra.connectors.common.callbacks.CallbackType.COMPENSATE;
import static io.narayana.lra.connectors.common.callbacks.CallbackType.COMPLETE;
import static io.narayana.lra.connectors.common.callbacks.CallbackType.FORGET;
import static io.narayana.lra.connectors.common.callbacks.CallbackType.STATUS;

import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.client.participant.Participant;
import io.narayana.lra.connectors.common.client.participant.ParticipantCallbackAddressResolver;
import io.narayana.lra.connectors.common.contracts.AfterLRACallback;
import io.narayana.lra.connectors.common.contracts.CompensateCallback;
import io.narayana.lra.connectors.common.contracts.CompleteCallback;
import io.narayana.lra.connectors.common.contracts.ForgetCallback;
import io.narayana.lra.connectors.common.contracts.ParticipantReply;
import io.narayana.lra.connectors.common.contracts.ParticipantRequest;
import io.narayana.lra.connectors.common.contracts.StatusCallback;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

/**
 * The MicroProfile LRA participant form: a bean whose callback methods are JAX-RS resource methods
 * (or plain methods when they do not need an endpoint of their own).
 * <p>
 * Callback methods that declare {@code @Path} and an HTTP verb are the participant's own endpoints
 * and are registered as such. Every other callback method has no endpoint, so it is served by the
 * transport-neutral callback proxy and registered as a contract address.
 */
public class MicroProfileLRAParticipant implements Participant {

    private final Class<?> javaClass;

    // callback methods without an endpoint of their own; they are invoked reflectively
    private final EnumMap<CallbackType, Method> callbackMethods = new EnumMap<>(CallbackType.class);

    // callback methods that declare their own JAX-RS endpoint, keyed by callback kind
    private final EnumMap<CallbackType, String> pathCallbacks = new EnumMap<>(CallbackType.class);

    private final Map<URI, ParticipantResult> participantStatusMap = new HashMap<>();

    private boolean asyncTermination;

    private Object instance;

    public MicroProfileLRAParticipant(Class<?> javaClass) {
        this.javaClass = javaClass;

        MicroProfileCallbackScan scan = MicroProfileCallbackScan.of(javaClass);

        callbackMethods.putAll(scan.generatedMethods());
        pathCallbacks.putAll(scan.ownEndpointPaths());
        asyncTermination = scan.asyncTermination();
    }

    @Override
    public String participantId() {
        return javaClass.getName();
    }

    @Override
    public boolean hasCallbacks() {
        return !callbackMethods.isEmpty() || !pathCallbacks.isEmpty();
    }

    @Override
    public boolean hasCallback(CallbackType type) {
        return callbackMethods.containsKey(type) || pathCallbacks.containsKey(type);
    }

    @Override
    public Class<?> participantClass() {
        return javaClass;
    }

    @Override
    public void setInstance(Object instance) {
        this.instance = instance;
    }

    @Override
    public ParticipantCallbacks terminationCallbacks(ParticipantCallbackAddressResolver resolver) {
        ParticipantCallbacks callbacks = resolver.resolve(this);
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
        ParticipantResult pending = new ParticipantResult(
                MicroProfileCallbackScan.getCompletionStageActualType(method));
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

        return switch (status) {
            case 200 -> entityOutcome(type, response);
            case 202 -> progress(type);
            case 404 -> error(type, "participant reports that the callback does not exist");
            case 409 -> failed(type, response);
            case 410 -> definitive(type);
            default -> error(type, "participant reports HTTP " + status);
        };
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

            return error(type, "participant callback returned an invalid participant status: " + text);
        }

        return error(type, "unsupported participant callback entity " + entity.getClass().getName());
    }

    // 409 means the termination failed; the body only has to be a valid ParticipantStatus, so it is
    // normalized to the failure state of the operation instead of being passed through
    private ParticipantReply failed(CallbackType type, Response response) {
        Object entity = response.hasEntity() ? response.getEntity() : null;

        if (entity != null && !(entity instanceof ParticipantStatus)
                && !isParticipantStatusName(entity.toString())) {
            return error(type, "participant reports HTTP 409 with an invalid participant status");
        }

        String message = entity == null ? null : entity.toString();

        return switch (type) {
            case COMPENSATE -> new CompensateCallback.Reply(ParticipantStatus.FailedToCompensate, message);
            case COMPLETE -> new CompleteCallback.Reply(ParticipantStatus.FailedToComplete, message);
            default -> error(type, "participant reports HTTP 409");
        };
    }

    private static boolean isParticipantStatusName(String value) {
        for (ParticipantStatus status : ParticipantStatus.values()) {
            if (status.name().equals(value.trim())) {
                return true;
            }
        }

        return false;
    }

    private ParticipantReply fromThrowable(CallbackType type, Throwable throwable) {
        if (throwable instanceof WebApplicationException webApplicationException) {
            return replyFor(type, webApplicationException.getResponse());
        }

        String message = message(throwable);

        return switch (type) {
            case COMPENSATE -> new CompensateCallback.Reply(ParticipantStatus.FailedToCompensate, message);
            case COMPLETE -> new CompleteCallback.Reply(ParticipantStatus.FailedToComplete, message);
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

        return reply(type, ParticipantStatus.valueOf(status.name()));
    }

    private static ParticipantReply reply(CallbackType type, ParticipantStatus outcome) {
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
            case COMPENSATE -> new CompensateCallback.Reply(ParticipantStatus.Compensating);
            case COMPLETE -> new CompleteCallback.Reply(ParticipantStatus.Completing);
            case STATUS -> new StatusCallback.Reply(ParticipantStatus.Compensating);
            // the contract has no in-progress reply for forget and after; an ack is the honest answer
            case FORGET, AFTER -> definitive(type);
        };
    }

    private static ParticipantReply definitive(CallbackType type) {
        return switch (type) {
            case COMPENSATE -> new CompensateCallback.Reply(ParticipantStatus.Compensated);
            case COMPLETE -> new CompleteCallback.Reply(ParticipantStatus.Completed);
            case STATUS -> new StatusCallback.Reply(ParticipantStatus.Compensated);
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

    // an asynchronous termination without a @Status or @Forget callback is invalid
    private void checkAsyncTermination(ParticipantCallbacks callbacks, boolean async) {
        if (async && callbacks.callback(STATUS) == null && callbacks.callback(FORGET) == null) {
            String logMsg = LRALogger.i18nLogger.error_asyncTerminationBeanMissStatusAndForget(javaClass);
            LRALogger.logger.warn(logMsg);
            throw new WebApplicationException(
                    Response.status(Response.Status.BAD_REQUEST).entity(logMsg).build());
        }
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
