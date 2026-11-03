/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.lifecycle;

import io.narayana.lra.AnnotationResolver;
import io.narayana.lra.PropagateToken;
import io.narayana.lra.api.Outcome;
import io.narayana.lra.connectors.common.client.interceptor.LRAAnnotationResolver;
import io.narayana.lra.connectors.common.client.invocation.LRATransactionMetadata;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.Forget;
import org.eclipse.microprofile.lra.annotation.Status;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.eclipse.microprofile.lra.annotation.ws.rs.Leave;

/**
 * {@link LRAAnnotationResolver} that reads the MicroProfile LRA annotations. It preserves the exact
 * behaviour that {@link io.narayana.lra.connectors.http.client.lifecycle.ServerLRAProcessor} had before it became
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
        Set<Integer> cancelOn = transactional.cancelOn().length != 0
                ? Arrays.stream(transactional.cancelOn()).map(Response.Status::getStatusCode).collect(Collectors.toSet())
                : null;
        Set<Outcome> cancelOnFamily = transactional.cancelOnFamily().length != 0
                ? Arrays.stream(transactional.cancelOnFamily())
                        .map(MicroProfileLRAAnnotationResolver::outcomeOf)
                        .collect(Collectors.toSet())
                : null;

        return new LRATransactionMetadata(LRA.Type.valueOf(transactional.value().name()),
                transactional.end(), timeLimitMillis, cancelOn, cancelOnFamily);
    }

    private static Outcome outcomeOf(Response.Status.Family family) {
        return switch (family) {
            case INFORMATIONAL -> Outcome.INFORMATIONAL;
            case SUCCESSFUL -> Outcome.SUCCESS;
            case REDIRECTION -> Outcome.REDIRECTION;
            case CLIENT_ERROR -> Outcome.CLIENT_ERROR;
            case SERVER_ERROR -> Outcome.SERVER_ERROR;
            case OTHER -> Outcome.OTHER;
        };
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
