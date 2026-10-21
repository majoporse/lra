/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client.internal.proxy.nonjaxrs;

import io.narayana.lra.LRAConstants;
import io.narayana.lra.client.internal.proxy.ParticipantProtocolRegistry;
import io.narayana.lra.client.internal.proxy.nonjaxrs.jandex.DotNames;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.AfterBeanDiscovery;
import jakarta.enterprise.inject.spi.BeanManager;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;

public class LegacyLRACDIExtension extends AbstractLRACDIExtension {

    private static final DotName JAX_RS_PATH = DotName.createSimple("jakarta.ws.rs.Path");

    public void observe(@Observes AfterBeanDiscovery event, BeanManager beanManager)
            throws IOException, ClassNotFoundException {
        Map<String, LRAParticipant> participants = new HashMap<>();

        if (!LRAConstants.isInterceptorMode()) {
            collectLegacyParticipants(participants, beanManager);
        }

        registerRegistry(event, beanManager, LRAParticipantRegistry.class,
                new LRAParticipantRegistry(participants));
        // the protocol registry is needed by both participant worlds
        registerRegistry(event, beanManager, ParticipantProtocolRegistry.class,
                new ParticipantProtocolRegistry());
    }

    private void collectLegacyParticipants(Map<String, LRAParticipant> participants, BeanManager beanManager)
            throws IOException, ClassNotFoundException {
        for (AnnotationInstance annotation : index().getAnnotations(JAX_RS_PATH)) {
            ClassInfo classInfo = declaringClass(annotation);

            if (classInfo == null) {
                continue;
            }

            Class<?> javaClass = loadClass(classInfo);

            if (!isConcrete(javaClass) || !isMicroProfileParticipant(classInfo)) {
                continue;
            }

            LRAParticipant participant = new LRAParticipant(javaClass);

            if (!participant.hasNonJaxRsMethods()) {
                continue;
            }

            participants.put(javaClass.getName(), participant);
            bindBeanInstance(javaClass, beanManager, participant::setInstance);
        }
    }

    private boolean isMicroProfileParticipant(ClassInfo classInfo) throws IOException {
        Map<DotName, java.util.List<AnnotationInstance>> annotations = annotationsOf(classInfo);

        if (!annotations.containsKey(DotNames.LRA)) {
            return false;
        }

        if (!annotations.containsKey(DotNames.COMPENSATE) && !annotations.containsKey(DotNames.AFTER_LRA)) {
            String message = String.format("%s: %s", classInfo.name(),
                    "The class contains an LRA method and no Compensate or AfterLRA method was found.");
            LRALogger.logger.error(message);
            throw new IllegalStateException(message);
        }

        return true;
    }
}
