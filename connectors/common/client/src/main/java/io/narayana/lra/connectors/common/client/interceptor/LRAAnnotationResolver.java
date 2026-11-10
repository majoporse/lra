/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.interceptor;

import io.narayana.lra.connectors.common.client.invocation.LRATransactionMetadata;
import java.lang.reflect.Method;

/**
 * Abstracts the source of the LRA annotations so that the lifecycle processor can be driven by the
 * MicroProfile LRA annotations. The implementation that reads those annotations lives with the
 * annotation artifact, because they carry JAX-RS types.
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
