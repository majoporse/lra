/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.discovery.cdi;

import io.narayana.lra.AnnotationResolver;
import io.narayana.lra.PropagateToken;
import io.narayana.lra.connectors.common.client.interceptor.LraManaged;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.context.NormalScope;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.AnnotatedType;
import jakarta.enterprise.inject.spi.Extension;
import jakarta.enterprise.inject.spi.ProcessAnnotatedType;
import jakarta.enterprise.inject.spi.configurator.AnnotatedMethodConfigurator;
import jakarta.enterprise.inject.spi.configurator.AnnotatedTypeConfigurator;
import jakarta.inject.Scope;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.eclipse.microprofile.lra.annotation.ws.rs.Leave;

/**
 * Makes {@link LraManaged} a working CDI interceptor binding for every method the interceptor is
 * responsible for.
 * <p>
 * {@code @LRA} was the binding before the marker existed, so two things are copied here:
 * <ul>
 * <li>the marker itself, for a class-level or method-level {@code @LRA}, for {@code @Leave} and for
 * a method-level {@code @PropagateToken} (neither of the latter two is an interceptor binding);</li>
 * <li>the real {@code @LRA} annotation declared on an interface or on an overridden superclass
 * method, because CDI does not inherit method-level interceptor bindings and the runtime resolver
 * still reads the annotation through {@link AnnotationResolver}.</li>
 * </ul>
 * The marker is inert: it is only consumed by the CDI binding machinery. The interceptor reads the
 * real annotations at runtime.
 */
public class LraBindingExtension implements Extension {

    public <T> void addBinding(@Observes ProcessAnnotatedType<T> event) {
        AnnotatedType<T> annotatedType = event.getAnnotatedType();
        Class<?> javaClass = annotatedType.getJavaClass();

        if (javaClass.isInterface() || Modifier.isAbstract(javaClass.getModifiers())) {
            return;
        }

        AnnotatedTypeConfigurator<T> configurator = event.configureAnnotatedType();
        boolean bound = annotatedType.getAnnotation(LRA.class) != null;

        if (bound) {
            configurator.add(LraManaged.Literal.INSTANCE);
        }

        for (AnnotatedMethodConfigurator<? super T> method : configurator.methods()) {
            Method javaMember = method.getAnnotated().getJavaMember();

            if (method.getAnnotated().getAnnotation(LRA.class) == null) {
                LRA inherited = AnnotationResolver.resolveAnnotation(LRA.class, javaMember);

                if (inherited != null) {
                    method.add(inherited);
                }
            }

            if (isManaged(javaMember)) {
                method.add(LraManaged.Literal.INSTANCE);
                bound = true;
            }
        }

        if (bound && !hasScope(annotatedType)) {
            configurator.add(Dependent.Literal.INSTANCE);
        }
    }

    /**
     * Mirrors {@code LraMethodMetadata.isManaged()} for the annotations that decide the binding.
     * A class-level {@code @PropagateToken} deliberately adds no marker of its own: it only
     * qualifies methods that are already managed, and a class-level marker would bind every method.
     */
    static boolean isManaged(Method method) {
        return AnnotationResolver.isAnnotationPresent(LRA.class, method)
                || AnnotationResolver.isAnnotationPresent(Leave.class, method)
                || AnnotationResolver.isAnnotationPresent(PropagateToken.class, method);
    }

    private static boolean hasScope(AnnotatedType<?> annotatedType) {
        return annotatedType.getAnnotations().stream()
                .anyMatch(annotation -> annotation.annotationType().isAnnotationPresent(Scope.class)
                        || annotation.annotationType().isAnnotationPresent(NormalScope.class));
    }
}
