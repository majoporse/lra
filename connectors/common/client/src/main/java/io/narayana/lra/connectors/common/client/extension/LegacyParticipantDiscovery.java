/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.extension;

import io.narayana.lra.connectors.common.client.extension.spi.LegacyParticipantFactory;
import io.narayana.lra.connectors.common.client.extension.spi.jandex.DotNames;
import io.narayana.lra.connectors.common.client.participant.Participant;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.inject.spi.BeanManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;

/**
 * Discovers the legacy (MicroProfile annotation) participants.
 * <p>
 * The scan walks the Jandex index for {@code @Path} classes that carry the MicroProfile LRA
 * annotations. Concrete subclasses of an annotated class are participants too, even when they
 * declare no annotation of their own.
 * <p>
 * The participant model itself is JAX-RS-bound, so the HTTP connector contributes it through a
 * {@link LegacyParticipantFactory}.
 */
public final class LegacyParticipantDiscovery {

    private static final DotName JAX_RS_PATH = DotName.createSimple("jakarta.ws.rs.Path");

    private final ParticipantDiscoverySupport support;
    private final LegacyParticipantFactory factory;

    LegacyParticipantDiscovery(Index index, LegacyParticipantFactory factory) {
        this.support = new ParticipantDiscoverySupport(index);
        this.factory = factory;
    }

    public static LegacyParticipantDiscovery create(Index index, LegacyParticipantFactory factory) {
        return new LegacyParticipantDiscovery(index, factory);
    }

    public void collect(Map<String, Participant> participants, BeanManager beanManager)
            throws IOException, ClassNotFoundException {
        if (factory == null) {
            return;
        }

        for (DotName root : annotatedClasses()) {
            for (ClassInfo candidate : candidates(root)) {
                collect(candidate, participants, beanManager);
            }
        }
    }

    private void collect(ClassInfo classInfo, Map<String, Participant> participants, BeanManager beanManager)
            throws IOException, ClassNotFoundException {
        Class<?> javaClass = support.loadClass(classInfo);

        if (!support.isConcrete(javaClass) || !isMicroProfileParticipant(classInfo)
                || participants.containsKey(javaClass.getName())) {
            return;
        }

        Participant participant = factory.create(javaClass);

        if (participant == null || !participant.hasCallbacks()) {
            return;
        }

        participants.put(participant.participantId(), participant);
        support.bindBeanInstance(javaClass, beanManager, participant::setInstance);
    }

    // the annotation instances only name the declaring class; a concrete subclass that inherits the
    // annotations is a participant as well
    private List<ClassInfo> candidates(DotName root) {
        List<ClassInfo> candidates = new ArrayList<>();
        add(candidates, root);

        for (ClassInfo subclass : support.index().getAllKnownSubclasses(root)) {
            candidates.add(subclass);
        }

        return candidates;
    }

    private void add(List<ClassInfo> candidates, DotName name) {
        ClassInfo classInfo = support.index().getClassByName(name);

        if (classInfo != null) {
            candidates.add(classInfo);
        }
    }

    private Set<DotName> annotatedClasses() {
        Set<DotName> roots = new LinkedHashSet<>();

        for (AnnotationInstance annotation : support.index().getAnnotations(JAX_RS_PATH)) {
            ClassInfo classInfo = support.declaringClass(annotation);

            if (classInfo != null) {
                roots.add(classInfo.name());
            }
        }

        return roots;
    }

    private boolean isMicroProfileParticipant(ClassInfo classInfo) throws IOException {
        Map<DotName, List<AnnotationInstance>> annotations = support.annotationsOf(classInfo);

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
