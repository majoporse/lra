/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.discovery.cdi;

import io.narayana.lra.connectors.common.client.discovery.MicroProfileParticipantFactory;
import io.narayana.lra.connectors.common.client.discovery.ParticipantDiscovery;
import io.narayana.lra.connectors.common.client.discovery.jandex.ClassPathIndexer;
import io.narayana.lra.connectors.common.client.discovery.jandex.DotNames;
import io.narayana.lra.connectors.common.client.discovery.jandex.JandexAnnotationResolver;
import io.narayana.lra.connectors.common.client.participant.LRAParticipantRegistry;
import io.narayana.lra.connectors.common.client.participant.Participant;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.context.NormalScope;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.AfterBeanDiscovery;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.BeforeBeanDiscovery;
import jakarta.enterprise.inject.spi.Extension;
import jakarta.enterprise.inject.spi.configurator.AnnotatedTypeConfigurator;
import jakarta.inject.Scope;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;

/**
 * Publishes the participant registry for portable CDI containers.
 * <p>
 * A participant is any bean that declares the MicroProfile LRA annotations. Its callback methods
 * may use either the MicroProfile shape or the callback contract, and one bean may mix both; the
 * scanner finds the class and {@link ParticipantDiscovery} builds the matching invocation
 * strategies.
 * <p>
 * Quarkus does not run portable CDI extensions; {@code ParticipantDiscoveryProcessor} is the
 * build-time counterpart. Both use {@link ParticipantScanner}.
 */
public class PortableParticipantDiscovery implements Extension {

    private Index index;

    /**
     * CDI (annotated discovery) does not consider a class with only {@code @Path} and an inherited
     * {@code @LRA} a bean, so the binding extension would never see it. Make those participants
     * beans explicitly; the binding extension then copies the inherited annotation.
     */
    public void addParticipantBeans(@Observes BeforeBeanDiscovery event) throws IOException {
        index = new ClassPathIndexer().createIndex();

        for (ClassInfo classInfo : index.getKnownClasses()) {
            if (Modifier.isInterface(classInfo.flags()) || Modifier.isAbstract(classInfo.flags())) {
                continue;
            }

            Map<DotName, List<AnnotationInstance>> annotations = JandexAnnotationResolver
                    .getAllAnnotationsFromClassInfoHierarchy(classInfo.name(), index);

            boolean managed = annotations.containsKey(DotNames.LRA)
                    || annotations.containsKey(DotNames.LEAVE)
                    || annotations.containsKey(DotNames.PROPAGATE_TOKEN);

            if (!managed || annotations.containsKey(DotNames.INTERCEPTOR)) {
                continue;
            }

            try {
                Class<?> javaClass = Class.forName(classInfo.name().toString(), false,
                        Thread.currentThread().getContextClassLoader());

                if (!hasBeanDefiningAnnotation(javaClass)) {
                    AnnotatedTypeConfigurator<?> configurator = event.addAnnotatedType(javaClass,
                            "lra-" + classInfo.name());
                    configurator.add(Dependent.Literal.INSTANCE);
                }
            } catch (ClassNotFoundException e) {
                // a Jandex entry that the deployment classloader cannot load; skip it
            }
        }
    }

    public void observe(@Observes AfterBeanDiscovery event, BeanManager beanManager)
            throws IOException, ClassNotFoundException {
        Index index = this.index != null ? this.index : new ClassPathIndexer().createIndex();
        Map<String, Participant> participants = new HashMap<>();

        new ParticipantDiscovery(index, microProfileParticipantFactory()).collect(participants, beanManager);

        event.addBean()
                .read(beanManager.createAnnotatedType(LRAParticipantRegistry.class))
                .beanClass(LRAParticipantRegistry.class)
                .scope(jakarta.enterprise.context.ApplicationScoped.class)
                .createWith(context -> new LRAParticipantRegistry(participants));
    }

    private static boolean hasBeanDefiningAnnotation(Class<?> javaClass) {
        for (Annotation annotation : javaClass.getAnnotations()) {
            Class<? extends Annotation> type = annotation.annotationType();

            if (type.isAnnotationPresent(Scope.class) || type.isAnnotationPresent(NormalScope.class)) {
                return true;
            }
        }

        return false;
    }

    private static MicroProfileParticipantFactory microProfileParticipantFactory() {
        for (MicroProfileParticipantFactory factory : ServiceLoader.load(MicroProfileParticipantFactory.class)) {
            return factory;
        }

        return null;
    }
}
