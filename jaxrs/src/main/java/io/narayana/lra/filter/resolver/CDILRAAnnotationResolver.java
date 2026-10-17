/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.filter.resolver;

import io.narayana.lra.AnnotationResolver;
import io.narayana.lra.api.LRA;
import java.lang.reflect.Method;
import java.time.Duration;

/**
 * {@link LRAAnnotationResolver} that reads the transport-neutral {@code io.narayana.lra.api}
 * annotations used by {@link io.narayana.lra.filter.interceptor.LRAInterceptor}.
 */
public class CDILRAAnnotationResolver implements LRAAnnotationResolver {

    @Override
    public LRATransactionMetadata resolveTransaction(Method method) {
        LRA.Transaction transaction = AnnotationResolver.resolveAnnotation(LRA.Transaction.class, method);

        if (transaction == null) {
            transaction = method.getDeclaringClass().getDeclaredAnnotation(LRA.Transaction.class);
        }

        if (transaction == null) {
            return null;
        }

        Long timeLimitMillis = transaction.timeLimit() != 0
                ? Duration.of(transaction.timeLimit(), transaction.timeUnit()).toMillis()
                : null;

        // the transport-neutral API expresses cancel conditions with @LRA.CancelOn, which is applied
        // by the interceptor itself, so no HTTP status based conditions are produced here
        return new LRATransactionMetadata(transaction.value(), transaction.end(), timeLimitMillis, null, null);
    }

    @Override
    public boolean isLeave(Method method) {
        return AnnotationResolver.isAnnotationPresent(LRA.Leave.class, method);
    }

    @Override
    public boolean isParticipantCallback(Method method) {
        return AnnotationResolver.isAnnotationPresent(LRA.Compensate.class, method)
                || AnnotationResolver.isAnnotationPresent(LRA.Complete.class, method)
                || AnnotationResolver.isAnnotationPresent(LRA.Status.class, method)
                || AnnotationResolver.isAnnotationPresent(LRA.Forget.class, method)
                || AnnotationResolver.isAnnotationPresent(LRA.AfterLRA.class, method);
    }

    @Override
    public boolean propagateToken(Method method) {
        return AnnotationResolver.isAnnotationPresent(LRA.PropagateToken.class, method)
                || method.getDeclaringClass().isAnnotationPresent(LRA.PropagateToken.class);
    }
}
