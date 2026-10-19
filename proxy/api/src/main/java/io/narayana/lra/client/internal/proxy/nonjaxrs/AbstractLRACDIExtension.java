/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client.internal.proxy.nonjaxrs;

import io.narayana.lra.client.internal.proxy.nonjaxrs.jandex.JandexAnnotationResolver;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.spi.AfterBeanDiscovery;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.Extension;
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
import org.jboss.jandex.Index;

abstract class AbstractLRACDIExtension implements Extension {

    private final ClassPathIndexer classPathIndexer = new ClassPathIndexer();
    private Index index;

    protected Index index() throws IOException {
        if (index == null) {
            index = classPathIndexer.createIndex();
        }

        return index;
    }

    protected ClassInfo declaringClass(AnnotationInstance annotation) {
        AnnotationTarget target = annotation.target();

        if (target.kind().equals(AnnotationTarget.Kind.CLASS)) {
            return target.asClass();
        } else if (target.kind().equals(AnnotationTarget.Kind.METHOD)) {
            return target.asMethod().declaringClass();
        }

        return null;
    }

    protected Class<?> loadClass(ClassInfo classInfo) throws ClassNotFoundException {
        String className = classInfo.name().toString();
        try {
            return getClass().getClassLoader().loadClass(className);
        } catch (ClassNotFoundException cnfe) {
            return Thread.currentThread().getContextClassLoader().loadClass(className);
        }
    }

    protected boolean isConcrete(Class<?> javaClass) {
        return !javaClass.isInterface() && !Modifier.isAbstract(javaClass.getModifiers());
    }

    protected Map<DotName, List<AnnotationInstance>> annotationsOf(ClassInfo classInfo) throws IOException {
        return JandexAnnotationResolver.getAllAnnotationsFromClassInfoHierarchy(classInfo.name(), index());
    }

    protected boolean registeredAsBean(Class<?> javaClass, BeanManager beanManager) {
        Set<Bean<?>> beans = beanManager.getBeans(javaClass, new AnnotationLiteral<Any>() {
        });
        return !beans.isEmpty();
    }

    protected void bindBeanInstance(Class<?> javaClass, BeanManager beanManager, Consumer<Object> setInstance) {
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

    protected <T> void registerRegistry(AfterBeanDiscovery event, BeanManager beanManager, Class<T> registryClass,
            T registry) {
        event.addBean()
                .read(beanManager.createAnnotatedType(registryClass))
                .beanClass(registryClass)
                .scope(jakarta.enterprise.context.ApplicationScoped.class)
                .createWith(context -> registry);
    }
}
