/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.annotation;

import io.narayana.lra.AnnotationResolver;
import io.narayana.lra.PropagateToken;
import io.narayana.lra.api.Outcome;
import io.narayana.lra.connectors.common.client.interceptor.LRAAnnotationResolver;
import io.narayana.lra.connectors.common.client.invocation.LraMethodMetadata;
import jakarta.enterprise.context.ApplicationScoped;
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
 * The MicroProfile LRA binding of the neutral {@link LRAAnnotationResolver}.
 * <p>
 * The MicroProfile annotations carry JAX-RS types ({@code cancelOn} is expressed as
 * {@code Response.Status} values), so this translator lives with the annotation artifact rather
 * than in the transport-neutral engine. It maps them onto the neutral cancel conditions.
 */
@ApplicationScoped
public class MicroProfileLRAAnnotationResolver implements LRAAnnotationResolver {

    @Override
    public LraMethodMetadata resolve(Method method) {
        LRA transactional = AnnotationResolver.resolveAnnotation(LRA.class, method);

        if (transactional == null) {
            transactional = method.getDeclaringClass().getDeclaredAnnotation(LRA.class);
        }

        boolean leave = isLeave(method);
        boolean participantCallback = isParticipantCallback(method);
        boolean propagateToken = propagateToken(method);

        if (transactional == null) {
            return new LraMethodMetadata(null, false, null, null, null, leave, participantCallback,
                    propagateToken);
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

        return new LraMethodMetadata(LRA.Type.valueOf(transactional.value().name()),
                transactional.end(), timeLimitMillis, cancelOn, cancelOnFamily, leave,
                participantCallback, propagateToken);
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

    private static boolean isLeave(Method method) {
        return AnnotationResolver.isAnnotationPresent(Leave.class, method);
    }

    private static boolean isParticipantCallback(Method method) {
        return AnnotationResolver.isAnnotationPresent(Compensate.class, method)
                || AnnotationResolver.isAnnotationPresent(Complete.class, method)
                || AnnotationResolver.isAnnotationPresent(Status.class, method)
                || AnnotationResolver.isAnnotationPresent(Forget.class, method)
                || AnnotationResolver.isAnnotationPresent(AfterLRA.class, method);
    }

    private static boolean propagateToken(Method method) {
        return AnnotationResolver.isAnnotationPresent(PropagateToken.class, method)
                || method.getDeclaringClass().isAnnotationPresent(PropagateToken.class);
    }
}
