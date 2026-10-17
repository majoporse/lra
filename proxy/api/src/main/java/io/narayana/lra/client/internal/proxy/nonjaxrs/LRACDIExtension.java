/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client.internal.proxy.nonjaxrs;

import io.narayana.lra.LRAConstants;
import io.narayana.lra.client.internal.proxy.nonjaxrs.jandex.DotNames;
import io.narayana.lra.client.internal.proxy.nonjaxrs.jandex.JandexAnnotationResolver;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.spi.AfterBeanDiscovery;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.DeploymentException;
import jakarta.enterprise.inject.spi.Extension;
import jakarta.enterprise.util.AnnotationLiteral;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;

/**
 * Collects the application's LRA participants (classes with non-JAX-RS callback methods) into the
 * {@link LRAParticipantRegistry}. Which classes qualify depends on who owns the server side LRA
 * processing: the {@code @Path} MicroProfile world with {@code ServerLRAFilter}, the
 * {@code @LRA.Transaction} beans with the CDI interceptor.
 */
public class LRACDIExtension implements Extension {

    private static final DotName JAX_RS_PATH = DotName.createSimple("jakarta.ws.rs.Path");

    private final ClassPathIndexer classPathIndexer = new ClassPathIndexer();
    private Index index;
    private final Map<String, LRAParticipant> participants = new HashMap<>();

    public void observe(@Observes AfterBeanDiscovery event, BeanManager beanManager)
            throws IOException, ClassNotFoundException {
        index = classPathIndexer.createIndex();

        if (LRAConstants.isInterceptorMode()) {
            discoverInterceptorModeParticipants(beanManager);
        } else {
            discoverLegacyParticipants(beanManager);
        }

        event.addBean()
                .read(beanManager.createAnnotatedType(LRAParticipantRegistry.class))
                .beanClass(LRAParticipantRegistry.class)
                .scope(ApplicationScoped.class)
                .createWith(context -> new LRAParticipantRegistry(participants));
    }

    private void discoverInterceptorModeParticipants(BeanManager beanManager) throws ClassNotFoundException {
        Set<String> registered = collectParticipantClasses(DotNames.LRA_TRANSACTION, beanManager);

        // their callbacks are built on the configured base URI; without it the coordinator could
        // never reach them back, so the application must not start
        if (!registered.isEmpty() && LRAConstants.getBaseUri().isEmpty()) {
            throw new DeploymentException(String.format(
                    "The transport-neutral LRA participants %s are enlisted with callback URIs built on the"
                            + " '%s' configuration property, which is not set. Set it to the base URI the LRA"
                            + " coordinator can use to reach this application, or remove the participant callbacks.",
                    registered.stream().sorted().collect(Collectors.joining(", ")),
                    LRAConstants.NARAYANA_LRA_BASE_URI_PROPERTY_NAME));
        }
    }

    private void discoverLegacyParticipants(BeanManager beanManager) throws ClassNotFoundException {
        collectParticipantClasses(JAX_RS_PATH, beanManager);
    }

    private Set<String> collectParticipantClasses(DotName annotationName, BeanManager beanManager)
            throws ClassNotFoundException {
        Set<String> registered = new HashSet<>();

        for (AnnotationInstance annotation : index.getAnnotations(annotationName)) {
            ClassInfo classInfo = declaringClass(annotation);

            if (classInfo != null) {
                LRAParticipant participant = asParticipant(classInfo);

                if (participant != null) {
                    participants.put(participant.getJavaClass().getName(), participant);
                    registered.add(participant.getJavaClass().getName());
                    bindBeanInstance(participant, beanManager);
                }
            }
        }

        return registered;
    }

    private LRAParticipant asParticipant(ClassInfo classInfo) throws ClassNotFoundException {
        Class<?> javaClass = loadClass(classInfo);

        if (javaClass.isInterface() || Modifier.isAbstract(javaClass.getModifiers())
                || !isLRAParticipant(classInfo)) {
            return null;
        }

        LRAParticipant participant = new LRAParticipant(javaClass);
        return participant.hasNonJaxRsMethods() ? participant : null;
    }

    private Class<?> loadClass(ClassInfo classInfo) throws ClassNotFoundException {
        String className = classInfo.name().toString();
        try {
            return getClass().getClassLoader().loadClass(className);
        } catch (ClassNotFoundException cnfe) {
            return Thread.currentThread().getContextClassLoader().loadClass(className);
        }
    }

    private ClassInfo declaringClass(AnnotationInstance annotation) {
        AnnotationTarget target = annotation.target();

        if (target.kind().equals(AnnotationTarget.Kind.CLASS)) {
            return target.asClass();
        } else if (target.kind().equals(AnnotationTarget.Kind.METHOD)) {
            return target.asMethod().declaringClass();
        }

        return null;
    }

    private void bindBeanInstance(LRAParticipant participant, BeanManager beanManager) {
        if (registeredAsBean(participant, beanManager)) {
            return;
        }

        try {
            participant.setInstance(participant.getJavaClass().getDeclaredConstructor().newInstance());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException
                | NoSuchMethodException e) {
            LRALogger.i18nLogger.error_cannotProcessParticipant(e);
        }
    }

    private boolean registeredAsBean(LRAParticipant participant, BeanManager beanManager) {
        Set<Bean<?>> participantBeans = beanManager.getBeans(participant.getJavaClass(), new AnnotationLiteral<Any>() {
        });
        return !participantBeans.isEmpty();
    }

    /**
     * @throws IllegalStateException if a MicroProfile {@code @LRA} class has no callback method
     */
    private boolean isLRAParticipant(ClassInfo classInfo) {
        Map<DotName, List<AnnotationInstance>> annotations = JandexAnnotationResolver
                .getAllAnnotationsFromClassInfoHierarchy(classInfo.name(), index);

        if (annotations.containsKey(DotNames.LRA)) {
            // an @LRA class is a participant by definition, so it must be compensable
            return isMicroProfileParticipant(classInfo, annotations);
        } else {
            // @LRA.Transaction may also just start or join an LRA without being a participant
            return isTransportNeutralParticipant(annotations);
        }
    }

    private boolean isMicroProfileParticipant(ClassInfo classInfo, Map<DotName, List<AnnotationInstance>> annotations) {
        if (!annotations.containsKey(DotNames.LRA) || !hasMicroProfileCallback(annotations)) {
            throw new IllegalStateException(String.format("%s: %s", classInfo.name(),
                    "The class contains an LRA method and no Compensate or AfterLRA method was found."));
        }

        return true;
    }

    private boolean isTransportNeutralParticipant(Map<DotName, List<AnnotationInstance>> annotations) {
        return annotations.containsKey(DotNames.LRA_TRANSACTION)
                && hasTransportNeutralCallback(annotations);
    }

    private boolean hasMicroProfileCallback(Map<DotName, List<AnnotationInstance>> annotations) {
        return annotations.containsKey(DotNames.COMPENSATE)
                || annotations.containsKey(DotNames.AFTER_LRA);
    }

    private boolean hasTransportNeutralCallback(Map<DotName, List<AnnotationInstance>> annotations) {
        return annotations.containsKey(DotNames.LRA_COMPENSATE)
                || annotations.containsKey(DotNames.LRA_AFTER_LRA);
    }
}
