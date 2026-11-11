/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.discovery.jandex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.MethodInfo;

/**
 * Collects the annotations of a class, its superclasses and its interfaces (transitively), because
 * a MicroProfile participant may declare its {@code @LRA} annotations on an implemented interface.
 */
public class JandexAnnotationResolver {

    public static Map<DotName, List<AnnotationInstance>> getAllAnnotationsFromClassInfoHierarchy(DotName name,
            IndexView index) {
        Map<DotName, List<AnnotationInstance>> annotations = new HashMap<>();

        collect(name, index, annotations);

        return annotations;
    }

    private static void collect(DotName name, IndexView index, Map<DotName, List<AnnotationInstance>> annotations) {
        if (name == null || name.equals(DotNames.OBJECT)) {
            return;
        }

        ClassInfo classInfo = index.getClassByName(name);

        if (classInfo == null) {
            return;
        }

        merge(annotations, classInfo.annotationsMap());

        for (DotName interfaceName : classInfo.interfaceNames()) {
            collect(interfaceName, index, annotations);
        }

        collect(classInfo.superName(), index, annotations);
    }

    /**
     * Whether the method, or the same method in a superclass or interface, carries the annotation.
     * This mirrors {@code AnnotationResolver}'s rule so the build-time binding decision and the
     * runtime metadata agree.
     */
    public static boolean isAnnotationPresent(DotName annotation, MethodInfo method, IndexView index) {
        return isAnnotationPresent(annotation, method, index.getClassByName(method.declaringClass().name()), index);
    }

    private static boolean isAnnotationPresent(DotName annotation, MethodInfo method, ClassInfo classInfo,
            IndexView index) {
        if (classInfo == null) {
            return false;
        }

        MethodInfo candidate = findMethod(classInfo, method);

        if (candidate != null && candidate.hasAnnotation(annotation)) {
            return true;
        }

        for (DotName interfaceName : classInfo.interfaceNames()) {
            if (isAnnotationPresent(annotation, method, index.getClassByName(interfaceName), index)) {
                return true;
            }
        }

        DotName superName = classInfo.superName();

        return superName != null && isAnnotationPresent(annotation, method, index.getClassByName(superName), index);
    }

    private static MethodInfo findMethod(ClassInfo classInfo, MethodInfo method) {
        for (MethodInfo candidate : classInfo.methods()) {
            if (candidate.name().equals(method.name())
                    && candidate.parameterTypes().equals(method.parameterTypes())) {
                return candidate;
            }
        }

        return null;
    }

    private static void merge(Map<DotName, List<AnnotationInstance>> target,
            Map<DotName, List<AnnotationInstance>> source) {
        source.forEach((name, instances) -> target.merge(name, new ArrayList<>(instances), (existing, added) -> {
            existing.addAll(added);
            return existing;
        }));
    }
}
