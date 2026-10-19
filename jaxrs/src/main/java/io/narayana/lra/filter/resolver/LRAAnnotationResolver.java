/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.filter.resolver;

import java.lang.reflect.Method;

/**
 * Abstracts the source of the LRA annotations so that
 * {@link io.narayana.lra.filter.core.ServerLRAProcessor} can be driven by either the MicroProfile LRA
 * annotations (used by the JAX-RS {@link io.narayana.lra.filter.ServerLRAFilter}) or the
 * transport-neutral {@code io.narayana.lra.api} annotations (used by
 * {@link io.narayana.lra.filter.interceptor.LRAInterceptor}).
 */
public interface LRAAnnotationResolver {

    /**
     * @param method the method being processed
     * @return the transaction attributes declared on the method, or {@code null} if the method does
     *         not start or join an LRA
     */
    LRATransactionMetadata resolveTransaction(Method method);

    /**
     * @param method the method being processed
     * @return whether the method requests leaving the incoming LRA
     */
    boolean isLeave(Method method);

    /**
     * @param method the method being processed
     * @return whether the method is a participant callback (compensate, complete, status, forget or
     *         after LRA)
     */
    boolean isParticipantCallback(Method method);

    /**
     * @param method the method being processed
     * @return whether a bearer token should be propagated to the LRA coordinator
     */
    boolean propagateToken(Method method);
}
