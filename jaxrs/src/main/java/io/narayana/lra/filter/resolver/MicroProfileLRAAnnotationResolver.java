/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.filter.resolver;

import io.narayana.lra.AnnotationResolver;
import io.narayana.lra.PropagateToken;
import io.narayana.lra.api.LRA.Propagation;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.Method;
import java.time.Duration;
import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.Forget;
import org.eclipse.microprofile.lra.annotation.Status;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.eclipse.microprofile.lra.annotation.ws.rs.Leave;

/**
 * {@link LRAAnnotationResolver} that reads the MicroProfile LRA annotations. It preserves the exact
 * behaviour that {@link io.narayana.lra.filter.core.ServerLRAProcessor} had before it became
 * annotation-agnostic.
 */
public class MicroProfileLRAAnnotationResolver implements LRAAnnotationResolver {

    @Override
    public LRATransactionMetadata resolveTransaction(Method method) {
        LRA transactional = AnnotationResolver.resolveAnnotation(LRA.class, method);

        if (transactional == null) {
            transactional = method.getDeclaringClass().getDeclaredAnnotation(LRA.class);
        }

        if (transactional == null) {
            return null;
        }

        Long timeLimitMillis = transactional.timeLimit() != 0
                ? Duration.of(transactional.timeLimit(), transactional.timeUnit()).toMillis()
                : null;
        Response.Status[] cancelOn = transactional.cancelOn().length != 0 ? transactional.cancelOn() : null;
        Response.Status.Family[] cancelOnFamily = transactional.cancelOnFamily().length != 0
                ? transactional.cancelOnFamily()
                : null;

        return new LRATransactionMetadata(Propagation.valueOf(transactional.value().name()),
                transactional.end(), timeLimitMillis, cancelOn, cancelOnFamily);
    }

    @Override
    public boolean isLeave(Method method) {
        return AnnotationResolver.isAnnotationPresent(Leave.class, method);
    }

    @Override
    public boolean isParticipantCallback(Method method) {
        return AnnotationResolver.isAnnotationPresent(Compensate.class, method)
                || AnnotationResolver.isAnnotationPresent(Complete.class, method)
                || AnnotationResolver.isAnnotationPresent(Status.class, method)
                || AnnotationResolver.isAnnotationPresent(Forget.class, method)
                || AnnotationResolver.isAnnotationPresent(AfterLRA.class, method);
    }

    @Override
    public boolean propagateToken(Method method) {
        return AnnotationResolver.isAnnotationPresent(PropagateToken.class, method)
                || method.getDeclaringClass().isAnnotationPresent(PropagateToken.class);
    }
}
