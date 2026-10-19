/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.api;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import jakarta.enterprise.util.Nonbinding;
import jakarta.interceptor.InterceptorBinding;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.time.temporal.ChronoUnit;

/**
 * The transport-neutral LRA API.
 * <p>
 * Everything the LRA protocol needs without depending on the transport: the interceptor binding
 * that starts or joins an LRA ({@link LRA.Transaction}), the participant callback markers
 * ({@link LRA.Complete}, {@link LRA.Compensate}, {@link LRA.AfterLRA}, ...) and the enums, result
 * wrapper and exception they share.
 * <p>
 * All types are nested here so that the API surface lives in a single file. This mirrors the
 * MicroProfile LRA convention of nesting types inside the {@code @LRA} annotation (for example
 * {@code LRA.Type}).
 */
public final class LRA {

    private LRA() {
    }

    /**
     * Marks a method as the completion callback of the LRA participant.
     * <p>
     * Transport-neutral replacement for the MicroProfile LRA {@code @Complete} annotation.
     */
    @Retention(RUNTIME)
    @Target(METHOD)
    public @interface Complete {
    }

    /**
     * Marks a method as the compensation callback of the LRA participant.
     * <p>
     * Transport-neutral replacement for the MicroProfile LRA {@code @Compensate} annotation.
     */
    @Retention(RUNTIME)
    @Target(METHOD)
    public @interface Compensate {
    }

    /**
     * Marks a method as the after-LRA callback of the LRA participant. The method is invoked once the
     * LRA has reached a final state and takes the LRA id and its final {@link LRA.TransactionStatus}.
     * <p>
     * Transport-neutral replacement for the MicroProfile LRA {@code @AfterLRA} annotation.
     */
    @Retention(RUNTIME)
    @Target(METHOD)
    public @interface AfterLRA {
    }

    /**
     * Marks a method as the forget callback of the LRA participant.
     * <p>
     * Transport-neutral replacement for the MicroProfile LRA {@code @Forget} annotation.
     */
    @Retention(RUNTIME)
    @Target(METHOD)
    public @interface Forget {
    }

    /**
     * Marks a method as the status callback of the LRA participant.
     * <p>
     * Transport-neutral replacement for the MicroProfile LRA {@code @Status} annotation.
     */
    @Retention(RUNTIME)
    @Target(METHOD)
    public @interface Status {
    }

    /**
     * Requests that the participant leaves the incoming LRA without ending it.
     * <p>
     * Transport-neutral replacement for the MicroProfile LRA {@code @Leave} annotation.
     */
    @Retention(RUNTIME)
    @Target({ TYPE, METHOD })
    public @interface Leave {
    }

    /**
     * Requests that a bearer token from the incoming invocation is propagated to the LRA coordinator.
     * <p>
     * Transport-neutral replacement for the Narayana {@code @PropagateToken} annotation.
     */
    @Retention(RUNTIME)
    @Target({ TYPE, METHOD })
    public @interface PropagateToken {
    }

    /**
     * Declares the {@link LRA.Outcome outcomes} for which an LRA managed by the LRA interceptor must
     * be cancelled instead of closed.
     * <p>
     * This is the transport-neutral counterpart of the MicroProfile LRA
     * {@code @LRA#cancelOn()} / {@code @LRA#cancelOnFamily()} attributes.
     * <p>
     * When the annotation is absent the LRA is closed whenever the method completes normally and is
     * cancelled when the method throws.
     */
    @Inherited
    @Retention(RUNTIME)
    @Target({ TYPE, METHOD })
    public @interface CancelOn {

        /**
         * @return the outcomes that trigger cancellation of the LRA
         */
        LRA.Outcome[] value();
    }

    /**
     * Marks a CDI bean (or a single method) as participating in the LRA protocol and controls the
     * lifecycle of the LRA that surrounds the invocation.
     * <p>
     * This annotation is both the CDI {@link InterceptorBinding interceptor binding} and the carrier
     * of the transaction attributes, so it is the transport-neutral replacement for the MicroProfile
     * LRA {@code @LRA} annotation. All members are {@link Nonbinding} so that a single interceptor
     * matches every configuration.
     */
    @InterceptorBinding
    @Retention(RUNTIME)
    @Target({ TYPE, METHOD })
    public @interface Transaction {

        /**
         * @return how the invocation relates to an existing LRA
         */
        @Nonbinding
        LRA.Propagation value() default LRA.Propagation.REQUIRED;

        /**
         * @return the time limit of a newly started LRA, expressed in {@link #timeUnit()}; {@code 0}
         *         means that the coordinator default is used
         */
        @Nonbinding
        long timeLimit() default 0;

        /**
         * @return the unit of {@link #timeLimit()}
         */
        @Nonbinding
        ChronoUnit timeUnit() default ChronoUnit.SECONDS;

        /**
         * @return whether the LRA should be ended (closed or cancelled) when the invocation completes
         */
        @Nonbinding
        boolean end() default true;
    }

    /**
     * Transport-neutral classification of the result of an intercepted method invocation, used to
     * decide whether the associated LRA should be closed or cancelled.
     */
    public enum Outcome {
        /** the invocation completed successfully */
        SUCCESS,

        /** the invocation failed because of the caller (equivalent to the HTTP 4xx family) */
        CLIENT_ERROR,

        /** the invocation failed because of the service (equivalent to the HTTP 5xx family) */
        SERVER_ERROR
    }

    /**
     * How a method annotated with {@link LRA.Transaction} relates to an existing LRA.
     * <p>
     * Transport-neutral replacement for the MicroProfile LRA {@code LRA.Type}.
     */
    public enum Propagation {
        /** the method must run within an LRA, joining an incoming one or starting a new one */
        REQUIRED,

        /** the method must run within a newly started LRA, suspending any incoming one */
        REQUIRES_NEW,

        /** the method must run within an incoming LRA, failing if there is none */
        MANDATORY,

        /** the method runs within an incoming LRA if there is one, otherwise without one */
        SUPPORTS,

        /** the method must run without an LRA, suspending any incoming one */
        NOT_SUPPORTED,

        /** the method must run without an LRA, failing if there is an incoming one */
        NEVER,

        /** a new child LRA is started, nested under any incoming one */
        NESTED
    }

    /**
     * The outcome reported by an LRA participant callback method.
     * <p>
     * Transport-neutral replacement for the MicroProfile LRA {@code ParticipantStatus} enum. The
     * constant names match the values expected by the LRA coordinator protocol.
     */
    public enum ParticipantOutcome {
        Active,
        Compensating,
        Compensated,
        FailedToCompensate,
        Completing,
        Completed,
        FailedToComplete
    }

    /**
     * The final state of an LRA, as reported to an {@link LRA.AfterLRA} callback.
     * <p>
     * Transport-neutral replacement for the MicroProfile LRA {@code LRAStatus} enum. The constant
     * names match the values expected by the LRA coordinator protocol.
     */
    public enum TransactionStatus {
        Active,
        Cancelling,
        Cancelled,
        FailedToCancel,
        Closing,
        Closed,
        FailedToClose
    }

    /**
     * The value returned by a method annotated with {@link LRA.Transaction}.
     * <p>
     * Since a CDI invocation, unlike a JAX-RS request, has no response object, this wrapper carries
     * both the entity produced by the method and a transport-neutral {@link LRA.Outcome} that the
     * interceptor uses to decide whether the associated LRA should be closed or cancelled.
     *
     * @param <T> the type of the entity produced by the intercepted method
     */
    public static final class Result<T> {

        private final T entity;
        private final LRA.Outcome outcome;

        private Result(T entity, LRA.Outcome outcome) {
            this.entity = entity;
            this.outcome = outcome;
        }

        public static <T> Result<T> of(T entity, LRA.Outcome outcome) {
            return new Result<>(entity, outcome);
        }

        public static <T> Result<T> success(T entity) {
            return new Result<>(entity, LRA.Outcome.SUCCESS);
        }

        public static <T> Result<T> clientError(T entity) {
            return new Result<>(entity, LRA.Outcome.CLIENT_ERROR);
        }

        public static <T> Result<T> serverError(T entity) {
            return new Result<>(entity, LRA.Outcome.SERVER_ERROR);
        }

        public T getEntity() {
            return entity;
        }

        public LRA.Outcome getOutcome() {
            return outcome;
        }
    }

    /**
     * Thrown by the {@code LRAInterceptor} when the LRA lifecycle associated with an intercepted
     * method completed with failed steps (for example a close, cancel or leave that did not succeed).
     * <p>
     * It is distinct from {@code LRAAbortException}: an abort means the invocation never ran (the
     * request processing stopped the method before it could execute), whereas a lifecycle failure
     * means the method ran but the surrounding LRA did not end cleanly.
     */
    public static class LifecycleException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public LifecycleException(String message) {
            super(message);
        }
    }
}
