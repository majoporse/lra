/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

import static io.narayana.lra.LRAConstants.AFTER;
import static io.narayana.lra.LRAConstants.COMPENSATE;
import static io.narayana.lra.LRAConstants.COMPLETE;
import static io.narayana.lra.LRAConstants.FORGET;
import static io.narayana.lra.LRAConstants.LEAVE;
import static io.narayana.lra.LRAConstants.STATUS;
import static io.narayana.lra.LRAConstants.TIMELIMIT_PARAM_NAME;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;

import io.narayana.lra.LRAConstants;
import io.narayana.lra.logging.LRALogger;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Link;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.Forget;
import org.eclipse.microprofile.lra.annotation.Status;
import org.eclipse.microprofile.lra.annotation.ws.rs.Leave;

/**
 * HTTP implementation of {@link CallbackRegistrar}.
 * Scans for JAX-RS annotations ({@code @Path}, {@code @GET}, {@code @PUT}, etc.)
 * and builds Link headers for callback registration.
 */
public class CallbackRegistrarHTTP implements CallbackRegistrar {

    private static final String LINK_TEXT = "Link";

    @Override
    public String buildRegistration(Class<?> compensatorClass, String uriPrefix, Long timeout) {
        Map<String, String> paths = getTerminationUris(compensatorClass, uriPrefix, timeout);
        return paths.getOrDefault(LINK_TEXT, "");
    }

    @Override
    public Map<String, String> getTerminationUris(Class<?> compensatorClass, String uriPrefix, Long timeout) {
        Map<String, String> paths = new HashMap<>();
        final boolean[] asyncTermination = { false };

        String timeoutValue = timeout != null ? Long.toString(timeout) : "0";

        Arrays.stream(compensatorClass.getMethods()).forEach(method -> {
            Path pathAnnotation = method.getAnnotation(Path.class);

            if (pathAnnotation != null) {

                if (checkMethod(paths, method, COMPENSATE, pathAnnotation,
                        method.getAnnotation(Compensate.class), uriPrefix) != 0) {
                    paths.put(TIMELIMIT_PARAM_NAME, timeoutValue);

                    if (isAsyncCompletion(method)) {
                        asyncTermination[0] = true;
                    }
                }

                if (checkMethod(paths, method, COMPLETE, pathAnnotation,
                        method.getAnnotation(Complete.class), uriPrefix) != 0) {
                    paths.put(TIMELIMIT_PARAM_NAME, timeoutValue);

                    if (isAsyncCompletion(method)) {
                        asyncTermination[0] = true;
                    }
                }
                checkMethod(paths, method, STATUS, pathAnnotation,
                        method.getAnnotation(Status.class), uriPrefix);
                checkMethod(paths, method, FORGET, pathAnnotation,
                        method.getAnnotation(Forget.class), uriPrefix);

                checkMethod(paths, method, LEAVE, pathAnnotation, method.getAnnotation(Leave.class), uriPrefix);
                checkMethod(paths, method, AFTER, pathAnnotation, method.getAnnotation(AfterLRA.class), uriPrefix);
            }
        });

        if (asyncTermination[0] && !paths.containsKey(STATUS) && !paths.containsKey(FORGET)) {
            String logMsg = LRALogger.i18nLogger.error_asyncTerminationBeanMissStatusAndForget(compensatorClass);
            LRALogger.logger.warn(logMsg);
            throw new WebApplicationException(
                    Response.status(BAD_REQUEST)
                            .entity(logMsg)
                            .build());
        }

        StringBuilder linkHeaderValue = new StringBuilder();

        if (!paths.isEmpty()) {
            paths.forEach((k, v) -> makeLink(linkHeaderValue, null, k, v));
            paths.put(LINK_TEXT, linkHeaderValue.toString());
        }

        return paths;
    }

    private static int checkMethod(Map<String, String> paths,
            Method method, String rel,
            Path pathAnnotation,
            Annotation annotationClass,
            String uriPrefix) {
        if (annotationClass == null) {
            return 0;
        }

        for (Annotation annotation : method.getDeclaredAnnotations()) {
            String name = annotation.annotationType().getName();

            if (name.equals(GET.class.getName()) ||
                    name.equals(PUT.class.getName()) ||
                    name.equals(POST.class.getName()) ||
                    name.equals(DELETE.class.getName())) {
                String pathValue = pathAnnotation.value();
                pathValue = pathValue.startsWith("/") ? pathValue : "/" + pathValue;
                String url = String.format("%s%s?%s=%s", uriPrefix, pathValue, LRAConstants.HTTP_METHOD_NAME, name);

                paths.put(rel, url);
                break;
            }
        }

        return 1;
    }

    private static boolean isAsyncCompletion(Method method) {
        // Async completion detection is not needed for the transport abstraction
        return false;
    }

    private static void makeLink(StringBuilder b, String uriPrefix, String key, String value) {
        if (value == null) {
            return;
        }

        Link link = Link.fromUri(value).title(key + " URI").rel(key).type(MediaType.TEXT_PLAIN).build();

        if (b.length() != 0) {
            b.append(',');
        }

        b.append(link);
    }
}
