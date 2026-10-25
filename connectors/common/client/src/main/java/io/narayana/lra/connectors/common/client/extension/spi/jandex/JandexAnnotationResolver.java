/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.extension.spi.jandex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;

/**
 * Collects the annotations of a class, its superclasses and its interfaces (transitively), because
 * a MicroProfile participant may declare its {@code @LRA} annotations on an implemented interface.
 */
public class JandexAnnotationResolver {

    public static Map<DotName, List<AnnotationInstance>> getAllAnnotationsFromClassInfoHierarchy(DotName name, Index index) {
        Map<DotName, List<AnnotationInstance>> annotations = new HashMap<>();

        collect(name, index, annotations);

        return annotations;
    }

    private static void collect(DotName name, Index index, Map<DotName, List<AnnotationInstance>> annotations) {
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

    private static void merge(Map<DotName, List<AnnotationInstance>> target,
            Map<DotName, List<AnnotationInstance>> source) {
        source.forEach((name, instances) -> target.merge(name, new ArrayList<>(instances), (existing, added) -> {
            existing.addAll(added);
            return existing;
        }));
    }
}
