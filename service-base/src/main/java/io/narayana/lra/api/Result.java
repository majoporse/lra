/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.api;

/**
 * The value returned by a method annotated with the MicroProfile {@code @LRA} annotation when the
 * invocation is intercepted by the CDI interceptor.
 * <p>
 * Since a CDI invocation, unlike a JAX-RS request, has no response object, this wrapper carries
 * both the entity produced by the method and a transport-neutral {@link Outcome} that the
 * interceptor uses to decide whether the associated LRA should be closed or cancelled.
 *
 * @param <T> the type of the entity produced by the intercepted method
 */
public final class Result<T> {

    private final T entity;
    private final Outcome outcome;

    private Result(T entity, Outcome outcome) {
        this.entity = entity;
        this.outcome = outcome;
    }

    public static <T> Result<T> of(T entity, Outcome outcome) {
        return new Result<>(entity, outcome);
    }

    public static <T> Result<T> success(T entity) {
        return new Result<>(entity, Outcome.SUCCESS);
    }

    public static <T> Result<T> clientError(T entity) {
        return new Result<>(entity, Outcome.CLIENT_ERROR);
    }

    public static <T> Result<T> serverError(T entity) {
        return new Result<>(entity, Outcome.SERVER_ERROR);
    }

    public T getEntity() {
        return entity;
    }

    public Outcome getOutcome() {
        return outcome;
    }
}
