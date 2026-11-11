/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.discovery;

import io.narayana.lra.connectors.common.client.discovery.jandex.DotNames;
import io.narayana.lra.connectors.common.client.discovery.jandex.JandexAnnotationResolver;
import io.narayana.lra.logging.LRALogger;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;

/**
 * Finds participant classes in a Jandex index.
 * <p>
 * The scan is pure: it reads annotations and class hierarchy, but never instantiates a class or
 * touches CDI. The portable CDI adapter and the Quarkus build step both use it, so the two
 * runtimes cannot disagree about which classes are participants.
 * <p>
 * Participants are found through the MicroProfile annotations only: an {@code @LRA} declaration
 * (class or method, including declarations inherited from an interface or a superclass) or a class
 * that declares the {@code @Compensate} / {@code @AfterLRA} callbacks without an {@code @LRA}.
 */
public final class ParticipantScanner {

    private static final Set<DotName> CALLBACK_ROOTS = Set.of(
            DotNames.COMPENSATE,
            DotNames.AFTER_LRA);

    public List<ParticipantDescriptor> scan(IndexView index) {
        Map<String, ParticipantDescriptor> participants = new LinkedHashMap<>();

        for (AnnotationInstance annotation : index.getAnnotations(DotNames.LRA)) {
            ClassInfo declaringClass = declaringClass(annotation);

            if (declaringClass != null) {
                collect(index, declaringClass, participants, true);
            }
        }

        for (DotName callback : CALLBACK_ROOTS) {
            for (AnnotationInstance annotation : index.getAnnotations(callback)) {
                ClassInfo declaringClass = declaringClass(annotation);

                if (declaringClass != null) {
                    collect(index, declaringClass, participants, false);
                }
            }
        }

        return new ArrayList<>(participants.values());
    }

    private void collect(IndexView index, ClassInfo declaredIn, Map<String, ParticipantDescriptor> participants,
            boolean requireLra) {
        for (ClassInfo candidate : candidates(index, declaredIn)) {
            if (!isConcrete(candidate) || !isParticipant(index, candidate, requireLra)) {
                continue;
            }

            participants.putIfAbsent(candidate.name().toString(),
                    new ParticipantDescriptor(candidate.name().toString()));
        }
    }

    // the annotation instances only name the declaring type; a concrete implementation or subclass
    // that inherits the annotations is a participant as well
    private List<ClassInfo> candidates(IndexView index, ClassInfo declaredIn) {
        Set<DotName> names = new LinkedHashSet<>();
        names.add(declaredIn.name());

        for (ClassInfo implementor : index.getAllKnownImplementors(declaredIn.name())) {
            names.add(implementor.name());
        }

        for (ClassInfo subclass : index.getAllKnownSubclasses(declaredIn.name())) {
            names.add(subclass.name());
        }

        List<ClassInfo> candidates = new ArrayList<>();

        for (DotName name : names) {
            ClassInfo classInfo = index.getClassByName(name);

            if (classInfo != null) {
                candidates.add(classInfo);
            }
        }

        return candidates;
    }

    private boolean isParticipant(IndexView index, ClassInfo classInfo, boolean requireLra) {
        Map<DotName, List<AnnotationInstance>> annotations = annotationsOf(index, classInfo);

        if (annotations.containsKey(DotNames.INTERCEPTOR)) {
            // an interceptor declares @LRA only to bind itself; it is not a participant
            return false;
        }

        if (requireLra && !annotations.containsKey(DotNames.LRA)) {
            return false;
        }

        if (!annotations.containsKey(DotNames.COMPENSATE) && !annotations.containsKey(DotNames.AFTER_LRA)) {
            if (requireLra) {
                String message = String.format("%s: %s", classInfo.name(),
                        "The class contains an LRA method and no Compensate or AfterLRA method was found.");
                LRALogger.logger.error(message);
                throw new IllegalStateException(message);
            }

            return false;
        }

        return true;
    }

    private static ClassInfo declaringClass(AnnotationInstance annotation) {
        AnnotationTarget target = annotation.target();

        if (target.kind().equals(AnnotationTarget.Kind.CLASS)) {
            return target.asClass();
        } else if (target.kind().equals(AnnotationTarget.Kind.METHOD)) {
            return target.asMethod().declaringClass();
        }

        return null;
    }

    private static boolean isConcrete(ClassInfo classInfo) {
        int flags = classInfo.flags();
        return !Modifier.isInterface(flags) && !Modifier.isAbstract(flags);
    }

    private static Map<DotName, List<AnnotationInstance>> annotationsOf(IndexView index, ClassInfo classInfo) {
        return JandexAnnotationResolver.getAllAnnotationsFromClassInfoHierarchy(classInfo.name(), index);
    }
}
