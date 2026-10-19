/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client.internal.proxy.nonjaxrs;

import io.narayana.lra.LRAConstants;
import io.narayana.lra.callbacks.CallbackFactory;
import io.narayana.lra.client.internal.proxy.callback.CallbackParticipantRegistry;
import io.narayana.lra.client.internal.proxy.callback.ContractParticipant;
import io.narayana.lra.client.internal.proxy.nonjaxrs.jandex.DotNames;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.AfterBeanDiscovery;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.DeploymentException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;

public class TransportNeutralLRACDIExtension extends AbstractLRACDIExtension {

    public void observe(@Observes AfterBeanDiscovery event, BeanManager beanManager)
            throws IOException, ClassNotFoundException {
        Map<String, ContractParticipant> participants = new HashMap<>();

        if (LRAConstants.isInterceptorMode()) {
            collectContractParticipants(participants, beanManager);
            requireCallbackBaseUri(participants);
        }

        registerRegistry(event, beanManager, CallbackParticipantRegistry.class,
                new CallbackParticipantRegistry(participants));
    }

    private void collectContractParticipants(Map<String, ContractParticipant> participants,
            BeanManager beanManager) throws IOException, ClassNotFoundException {
        for (AnnotationInstance annotation : index().getAnnotations(DotNames.LRA_TRANSACTION)) {
            ClassInfo classInfo = declaringClass(annotation);

            if (classInfo == null) {
                continue;
            }

            Class<?> javaClass = loadClass(classInfo);

            if (!isConcrete(javaClass) || !isContractParticipant(classInfo)) {
                continue;
            }

            ContractParticipant participant;

            try {
                participant = new ContractParticipant(javaClass);
            } catch (RuntimeException e) {
                throw new DeploymentException(String.format(
                        "Invalid transport-neutral LRA participant %s: %s", javaClass.getName(), e.getMessage()), e);
            }

            participants.put(participant.participantId(), participant);
            bindBeanInstance(javaClass, beanManager, participant::setInstance);
        }
    }

    private boolean isContractParticipant(ClassInfo classInfo) throws IOException {
        Map<DotName, java.util.List<AnnotationInstance>> annotations = annotationsOf(classInfo);

        return annotations.containsKey(DotNames.LRA_COMPENSATE)
                || annotations.containsKey(DotNames.LRA_COMPLETE)
                || annotations.containsKey(DotNames.LRA_STATUS)
                || annotations.containsKey(DotNames.LRA_FORGET)
                || annotations.containsKey(DotNames.LRA_AFTER_LRA);
    }

    private void requireCallbackBaseUri(Map<String, ContractParticipant> participants) {
        if (participants.isEmpty() || isKafkaProtocol()) {
            return;
        }

        if (LRAConstants.getBaseUri().isEmpty()) {
            throw new DeploymentException(String.format(
                    "The transport-neutral LRA participants %s are enlisted with callback URIs built on the"
                            + " '%s' configuration property, which is not set. Set it to the base URI the LRA"
                            + " coordinator can use to reach this application, or remove the participant callbacks.",
                    participants.keySet().stream().sorted().collect(Collectors.joining(", ")),
                    LRAConstants.NARAYANA_LRA_BASE_URI_PROPERTY_NAME));
        }
    }

    private boolean isKafkaProtocol() {
        try {
            return new CallbackFactory().isKafka();
        } catch (RuntimeException e) {
            throw new DeploymentException(
                    "Cannot resolve the LRA callback protocol; configure '" + CallbackFactory.PROTOCOL_PROPERTY + "'"
                            + " and '" + CallbackFactory.SERVICE_TOPIC_PROPERTY + "' consistently",
                    e);
        }
    }
}
