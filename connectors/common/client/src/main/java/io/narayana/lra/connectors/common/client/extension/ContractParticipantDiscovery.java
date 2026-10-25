/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.extension;

import io.narayana.lra.connectors.common.client.extension.spi.jandex.DotNames;
import io.narayana.lra.connectors.common.client.participant.LRAParticipant;
import io.narayana.lra.connectors.common.client.participant.Participant;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.DeploymentException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;

/**
 * Discovers the transport-neutral participants: classes annotated with {@code @LRA.Transaction}
 * and at least one {@code @LRA} callback. Their signatures are strict, so an invalid participant
 * fails deployment with the offending method named.
 */
final class ContractParticipantDiscovery {

    private final ParticipantDiscoverySupport support;

    ContractParticipantDiscovery(Index index) {
        this.support = new ParticipantDiscoverySupport(index);
    }

    void collect(Map<String, Participant> participants, BeanManager beanManager)
            throws IOException, ClassNotFoundException {
        for (AnnotationInstance annotation : support.index().getAnnotations(DotNames.LRA_TRANSACTION)) {
            ClassInfo classInfo = support.declaringClass(annotation);

            if (classInfo == null) {
                continue;
            }

            collect(classInfo, participants, beanManager);
        }
    }

    private void collect(ClassInfo classInfo, Map<String, Participant> participants, BeanManager beanManager)
            throws IOException, ClassNotFoundException {
        Class<?> javaClass = support.loadClass(classInfo);

        if (!support.isConcrete(javaClass) || !isContractParticipant(classInfo)
                || participants.containsKey(javaClass.getName())) {
            return;
        }

        LRAParticipant participant;

        try {
            participant = new LRAParticipant(javaClass);
        } catch (RuntimeException e) {
            throw new DeploymentException(String.format(
                    "Invalid transport-neutral LRA participant %s: %s", javaClass.getName(), e.getMessage()), e);
        }

        participants.put(participant.participantId(), participant);
        support.bindBeanInstance(javaClass, beanManager, participant::setInstance);
    }

    private boolean isContractParticipant(ClassInfo classInfo) throws IOException {
        Map<DotName, List<AnnotationInstance>> annotations = support.annotationsOf(classInfo);

        return annotations.containsKey(DotNames.LRA_COMPENSATE)
                || annotations.containsKey(DotNames.LRA_COMPLETE)
                || annotations.containsKey(DotNames.LRA_STATUS)
                || annotations.containsKey(DotNames.LRA_FORGET)
                || annotations.containsKey(DotNames.LRA_AFTER_LRA);
    }
}
