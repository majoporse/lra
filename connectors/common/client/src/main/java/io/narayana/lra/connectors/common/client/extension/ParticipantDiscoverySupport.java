/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.extension;

import io.narayana.lra.connectors.common.client.extension.spi.jandex.JandexAnnotationResolver;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.util.AnnotationLiteral;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;

/**
 * The Jandex and bean-manager helpers both participant discoveries need.
 */
final class ParticipantDiscoverySupport {

    private final IndexView index;

    ParticipantDiscoverySupport(IndexView index) {
        this.index = index;
    }

    IndexView index() {
        return index;
    }

    ClassInfo declaringClass(AnnotationInstance annotation) {
        AnnotationTarget target = annotation.target();

        if (target.kind().equals(AnnotationTarget.Kind.CLASS)) {
            return target.asClass();
        } else if (target.kind().equals(AnnotationTarget.Kind.METHOD)) {
            return target.asMethod().declaringClass();
        }

        return null;
    }

    Class<?> loadClass(ClassInfo classInfo) throws ClassNotFoundException {
        return loadClass(classInfo.name().toString());
    }

    Class<?> loadClass(String className) throws ClassNotFoundException {
        try {
            return getClass().getClassLoader().loadClass(className);
        } catch (ClassNotFoundException cnfe) {
            return Thread.currentThread().getContextClassLoader().loadClass(className);
        }
    }

    boolean isConcrete(Class<?> javaClass) {
        return !javaClass.isInterface() && !Modifier.isAbstract(javaClass.getModifiers());
    }

    Map<DotName, List<AnnotationInstance>> annotationsOf(ClassInfo classInfo) throws IOException {
        return JandexAnnotationResolver.getAllAnnotationsFromClassInfoHierarchy(classInfo.name(), index);
    }

    void bindBeanInstance(Class<?> javaClass, BeanManager beanManager, Consumer<Object> setInstance) {
        if (registeredAsBean(javaClass, beanManager)) {
            return;
        }

        try {
            setInstance.accept(javaClass.getDeclaredConstructor().newInstance());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException
                | NoSuchMethodException e) {
            LRALogger.i18nLogger.error_cannotProcessParticipant(e);
        }
    }

    private static boolean registeredAsBean(Class<?> javaClass, BeanManager beanManager) {
        Set<Bean<?>> beans = beanManager.getBeans(javaClass, new AnnotationLiteral<Any>() {
        });
        return !beans.isEmpty();
    }
}
