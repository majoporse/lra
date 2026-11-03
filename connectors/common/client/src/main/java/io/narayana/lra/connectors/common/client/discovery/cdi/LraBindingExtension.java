/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.discovery.cdi;

import io.narayana.lra.AnnotationResolver;
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

/**
 * Makes {@link LRA} a working CDI interceptor binding for participants whose transaction annotation
 * is declared on an interface or on an overridden superclass method.
 * <p>
 * CDI does not inherit method-level interceptor bindings from an interface or from a method the
 * bean overrides, while the MicroProfile specification requires the annotation to be honoured in
 * those positions. The extension copies the real annotation instance (the one declared in the
 * hierarchy) onto the implementation method, so the binding and the values are both preserved. The
 * interceptor still reads the annotation through {@link AnnotationResolver}.
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

        for (AnnotatedMethodConfigurator<? super T> method : configurator.methods()) {
            Method javaMember = method.getAnnotated().getJavaMember();

            if (method.getAnnotated().getAnnotation(LRA.class) != null) {
                bound = true;
                continue;
            }

            LRA inherited = AnnotationResolver.resolveAnnotation(LRA.class, javaMember);

            if (inherited != null) {
                method.add(inherited);
                bound = true;
            }
        }

        if (bound && !hasScope(annotatedType)) {
            configurator.add(Dependent.Literal.INSTANCE);
        }
    }

    private static boolean hasScope(AnnotatedType<?> annotatedType) {
        return annotatedType.getAnnotations().stream()
                .anyMatch(annotation -> annotation.annotationType().isAnnotationPresent(Scope.class)
                        || annotation.annotationType().isAnnotationPresent(NormalScope.class));
    }
}
