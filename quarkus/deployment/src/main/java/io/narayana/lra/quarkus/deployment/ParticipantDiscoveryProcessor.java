/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.quarkus.deployment;

import io.narayana.lra.connectors.common.callbacks.CallbackAddress;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.client.callback.ParticipantCallbackDispatcher;
import io.narayana.lra.connectors.common.client.coordinator.LRAClient;
import io.narayana.lra.connectors.common.client.discovery.MicroProfileParticipantFactory;
import io.narayana.lra.connectors.common.client.discovery.ParticipantDescriptor;
import io.narayana.lra.connectors.common.client.discovery.ParticipantScanner;
import io.narayana.lra.connectors.common.client.discovery.jandex.DotNames;
import io.narayana.lra.connectors.common.client.discovery.jandex.JandexAnnotationResolver;
import io.narayana.lra.connectors.common.client.participant.LRAParticipantRegistry;
import io.narayana.lra.connectors.common.client.participant.ParticipantCallbackAddressResolver;
import io.narayana.lra.connectors.common.contracts.ParticipantReply;
import io.narayana.lra.connectors.common.contracts.ParticipantRequest;
import io.narayana.lra.quarkus.runtime.LraQuarkusRecorder;
import io.quarkus.arc.deployment.AnnotationsTransformerBuildItem;
import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.arc.deployment.UnremovableBeanBuildItem;
import io.quarkus.arc.processor.AnnotationsTransformer;
import io.quarkus.arc.processor.AnnotationsTransformer.TransformationContext;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.IndexDependencyBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ServiceProviderBuildItem;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.Forget;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;
import org.eclipse.microprofile.lra.annotation.Status;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.MethodInfo;

public class ParticipantDiscoveryProcessor {

    private static final DotName CALLBACK_ADDRESS = DotName.createSimple(CallbackAddress.class.getName());
    private static final DotName PARTICIPANT_REQUEST = DotName.createSimple(ParticipantRequest.class.getName());
    private static final DotName PARTICIPANT_REPLY = DotName.createSimple(ParticipantReply.class.getName());

    @BuildStep
    void indexCommonApi(BuildProducer<IndexDependencyBuildItem> indexDependencies) {
        indexDependencies.produce(
                new IndexDependencyBuildItem("org.eclipse.microprofile.lra", "microprofile-lra-api"));
        indexDependencies.produce(
                new IndexDependencyBuildItem("org.jboss.narayana.lra", "lra-connector-http-client"));
    }

    /**
     * CDI does not inherit method-level interceptor bindings from an interface or from an overridden
     * superclass method, while the MicroProfile specification requires {@code @LRA} to be honoured
     * there. Bind the interceptor through {@code @LraManaged}, which is added for every method that
     * declares {@code @LRA}, {@code @Leave} or a method-level {@code @PropagateToken}, and copy a
     * class-level {@code @LRA} declared in the hierarchy onto the implementation.
     */
    @BuildStep
    AnnotationsTransformerBuildItem bindLraOnImplementations(CombinedIndexBuildItem index) {
        IndexView indexView = index.getIndex();

        return new AnnotationsTransformerBuildItem(new AnnotationsTransformer() {
            @Override
            public boolean appliesTo(AnnotationTarget.Kind kind) {
                return kind == AnnotationTarget.Kind.CLASS || kind == AnnotationTarget.Kind.METHOD;
            }

            @Override
            public void transform(TransformationContext context) {
                if (context.getTarget().kind() == AnnotationTarget.Kind.METHOD) {
                    transformMethod(context, indexView);
                } else {
                    transformClass(context, indexView);
                }
            }
        });
    }

    private static void transformMethod(TransformationContext context, IndexView index) {
        if (isManaged(context.getTarget().asMethod(), index)) {
            context.transform().add(DotNames.LRA_MANAGED).done();
        }
    }

    private static void transformClass(TransformationContext context, IndexView index) {
        ClassInfo classInfo = context.getTarget().asClass();

        if (Modifier.isInterface(classInfo.flags()) || Modifier.isAbstract(classInfo.flags())) {
            return;
        }

        if (hasClassLevelLra(classInfo)) {
            context.transform().add(DotNames.LRA_MANAGED).done();
            return;
        }

        if (JandexAnnotationResolver.getAllAnnotationsFromClassInfoHierarchy(classInfo.name(), index)
                .containsKey(DotNames.LRA)) {
            context.transform().add(DotNames.LRA).add(DotNames.LRA_MANAGED).done();
        }
    }

    /**
     * Mirrors {@code LraMethodMetadata.isManaged()} for the annotations that decide the binding.
     */
    static boolean isManaged(MethodInfo method, IndexView index) {
        return JandexAnnotationResolver.isAnnotationPresent(DotNames.LRA, method, index)
                || JandexAnnotationResolver.isAnnotationPresent(DotNames.LEAVE, method, index)
                || JandexAnnotationResolver.isAnnotationPresent(DotNames.PROPAGATE_TOKEN, method, index);
    }

    @BuildStep
    UnremovableBeanBuildItem keepConnectorBeans() {
        return UnremovableBeanBuildItem.beanTypes(LRAClient.class, ParticipantCallbackAddressResolver.class);
    }

    @BuildStep
    @Record(ExecutionTime.STATIC_INIT)
    void discoverParticipants(
            CombinedIndexBuildItem index,
            LraQuarkusRecorder recorder,
            BuildProducer<SyntheticBeanBuildItem> syntheticBeans,
            BuildProducer<UnremovableBeanBuildItem> unremovable,
            BuildProducer<ReflectiveClassBuildItem> reflective) {
        List<String> participants = new ParticipantScanner().scan(index.getIndex()).stream()
                .map(ParticipantDescriptor::className)
                .toList();

        syntheticBeans.produce(SyntheticBeanBuildItem.configure(LRAParticipantRegistry.class)
                .scope(ApplicationScoped.class)
                .addType(LRAParticipantRegistry.class)
                .addType(ParticipantCallbackDispatcher.class)
                .unremovable()
                .runtimeValue(recorder.participantRegistry(participants))
                .done());

        if (!participants.isEmpty()) {
            unremovable.produce(UnremovableBeanBuildItem.beanClassNames(participants.toArray(String[]::new)));
            reflective.produce(ReflectiveClassBuildItem.builder(participants)
                    .constructors()
                    .methods()
                    .fields()
                    .build());
        }
    }

    @BuildStep
    void nativeImageSupport(
            CombinedIndexBuildItem index,
            BuildProducer<ReflectiveClassBuildItem> reflective,
            BuildProducer<ServiceProviderBuildItem> serviceProviders) throws IOException {
        reflective.produce(ReflectiveClassBuildItem.builder(reflectiveTypes(index.getIndex()))
                .constructors()
                .methods()
                .fields()
                .build());

        serviceProviders.produce(ServiceProviderBuildItem
                .allProvidersFromClassPath("com.fasterxml.jackson.databind.Module"));
        serviceProviders.produce(ServiceProviderBuildItem
                .allProvidersFromClassPath(MicroProfileParticipantFactory.class.getName()));
    }

    private static List<String> reflectiveTypes(IndexView index) {
        List<String> types = new ArrayList<>(List.of(
                CallbackAddress.class.getName(),
                ParticipantRequest.class.getName(),
                ParticipantReply.class.getName(),
                ParticipantCallbacks.class.getName(),
                CallbackType.class.getName(),
                ParticipantStatus.class.getName(),
                LRAStatus.class.getName(),
                org.eclipse.microprofile.lra.annotation.ws.rs.LRA.class.getName(),
                Compensate.class.getName(),
                Complete.class.getName(),
                Status.class.getName(),
                Forget.class.getName(),
                AfterLRA.class.getName()));

        addClassNames(types, index.getAllKnownImplementors(CALLBACK_ADDRESS));
        addClassNames(types, index.getAllKnownSubclasses(PARTICIPANT_REQUEST));
        addClassNames(types, index.getAllKnownSubclasses(PARTICIPANT_REPLY));

        return types.stream().distinct().toList();
    }

    private static void addClassNames(List<String> types, Iterable<ClassInfo> classes) {
        for (ClassInfo classInfo : classes) {
            types.add(classInfo.name().toString());
        }
    }

    private static boolean hasClassLevelLra(ClassInfo classInfo) {
        return classInfo.annotationsMap().getOrDefault(DotNames.LRA, List.of()).stream()
                .anyMatch(annotation -> annotation.target().kind() == org.jboss.jandex.AnnotationTarget.Kind.CLASS);
    }
}
