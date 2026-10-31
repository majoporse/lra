/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.extension;

import io.narayana.lra.connectors.common.client.extension.spi.jandex.DotNames;
import io.narayana.lra.connectors.common.client.extension.spi.jandex.JandexAnnotationResolver;
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
 */
public final class ParticipantScanner {

    private static final DotName JAX_RS_PATH = DotName.createSimple("jakarta.ws.rs.Path");

    private static final Set<DotName> CONTRACT_CALLBACKS = Set.of(
            DotNames.LRA_COMPENSATE,
            DotNames.LRA_COMPLETE,
            DotNames.LRA_STATUS,
            DotNames.LRA_FORGET,
            DotNames.LRA_AFTER_LRA);

    public List<ParticipantDescriptor> scan(IndexView index, ParticipantForm form) {
        return switch (form) {
            case CONTRACT -> scanContract(index);
            case LEGACY -> scanLegacy(index);
        };
    }

    private List<ParticipantDescriptor> scanContract(IndexView index) {
        Map<String, ParticipantDescriptor> participants = new LinkedHashMap<>();

        for (AnnotationInstance annotation : index.getAnnotations(DotNames.LRA_TRANSACTION)) {
            ClassInfo classInfo = declaringClass(annotation);

            if (classInfo == null || !isConcrete(classInfo) || !hasAny(annotationsOf(index, classInfo), CONTRACT_CALLBACKS)) {
                continue;
            }

            participants.putIfAbsent(classInfo.name().toString(),
                    new ParticipantDescriptor(classInfo.name().toString(), ParticipantForm.CONTRACT));
        }

        return new ArrayList<>(participants.values());
    }

    private List<ParticipantDescriptor> scanLegacy(IndexView index) {
        Map<String, ParticipantDescriptor> participants = new LinkedHashMap<>();

        for (ClassInfo root : annotatedClasses(index)) {
            for (ClassInfo candidate : candidates(index, root)) {
                if (!isConcrete(candidate) || !isLegacyParticipant(index, candidate)) {
                    continue;
                }

                participants.putIfAbsent(candidate.name().toString(),
                        new ParticipantDescriptor(candidate.name().toString(), ParticipantForm.LEGACY));
            }
        }

        return new ArrayList<>(participants.values());
    }

    private List<ClassInfo> annotatedClasses(IndexView index) {
        Set<DotName> roots = new LinkedHashSet<>();

        for (AnnotationInstance annotation : index.getAnnotations(JAX_RS_PATH)) {
            ClassInfo classInfo = declaringClass(annotation);

            if (classInfo != null) {
                roots.add(classInfo.name());
            }
        }

        List<ClassInfo> classes = new ArrayList<>();

        for (DotName root : roots) {
            ClassInfo classInfo = index.getClassByName(root);

            if (classInfo != null) {
                classes.add(classInfo);
            }
        }

        return classes;
    }

    // the annotation instances only name the declaring class; a concrete subclass that inherits the
    // annotations is a participant as well
    private List<ClassInfo> candidates(IndexView index, ClassInfo root) {
        List<ClassInfo> candidates = new ArrayList<>();
        candidates.add(root);
        candidates.addAll(index.getAllKnownSubclasses(root.name()));
        return candidates;
    }

    private boolean isLegacyParticipant(IndexView index, ClassInfo classInfo) {
        Map<DotName, List<AnnotationInstance>> annotations = annotationsOf(index, classInfo);

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

    private static boolean hasAny(Map<DotName, List<AnnotationInstance>> annotations, Set<DotName> names) {
        for (DotName name : names) {
            if (annotations.containsKey(name)) {
                return true;
            }
        }

        return false;
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
