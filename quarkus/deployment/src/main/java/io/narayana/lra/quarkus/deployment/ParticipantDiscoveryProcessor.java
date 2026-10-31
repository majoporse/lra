/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.quarkus.deployment;

import io.narayana.lra.api.LRA;
import io.narayana.lra.connectors.common.callbacks.CallbackAddress;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.client.LRAClient;
import io.narayana.lra.connectors.common.client.ParticipantCallbackAddressResolver;
import io.narayana.lra.connectors.common.client.ParticipantCallbackDispatcher;
import io.narayana.lra.connectors.common.client.extension.ParticipantDescriptor;
import io.narayana.lra.connectors.common.client.extension.ParticipantForm;
import io.narayana.lra.connectors.common.client.extension.ParticipantScanner;
import io.narayana.lra.connectors.common.client.extension.spi.LegacyParticipantFactory;
import io.narayana.lra.connectors.common.client.participant.LRAParticipantRegistry;
import io.narayana.lra.connectors.common.contracts.ParticipantReply;
import io.narayana.lra.connectors.common.contracts.ParticipantRequest;
import io.narayana.lra.quarkus.runtime.LraQuarkusRecorder;
import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.arc.deployment.UnremovableBeanBuildItem;
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
import java.util.ArrayList;
import java.util.List;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.Forget;
import org.eclipse.microprofile.lra.annotation.Status;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;

public class ParticipantDiscoveryProcessor {

    private static final DotName CALLBACK_ADDRESS = DotName.createSimple(CallbackAddress.class.getName());
    private static final DotName PARTICIPANT_REQUEST = DotName.createSimple(ParticipantRequest.class.getName());
    private static final DotName PARTICIPANT_REPLY = DotName.createSimple(ParticipantReply.class.getName());

    @BuildStep
    void indexCommonApi(BuildProducer<IndexDependencyBuildItem> indexDependencies) {
        indexDependencies.produce(
                new IndexDependencyBuildItem("org.eclipse.microprofile.lra", "microprofile-lra-api"));
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
        boolean interceptorMode = ConfigProvider.getConfig()
                .getOptionalValue("lra.server.filter.interceptor", Boolean.class)
                .orElse(false);
        ParticipantForm form = interceptorMode ? ParticipantForm.CONTRACT : ParticipantForm.LEGACY;
        List<String> participants = new ParticipantScanner().scan(index.getIndex(), form).stream()
                .map(ParticipantDescriptor::className)
                .toList();

        syntheticBeans.produce(SyntheticBeanBuildItem.configure(LRAParticipantRegistry.class)
                .scope(ApplicationScoped.class)
                .addType(LRAParticipantRegistry.class)
                .addType(ParticipantCallbackDispatcher.class)
                .unremovable()
                .runtimeValue(recorder.participantRegistry(form, participants))
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
                .allProvidersFromClassPath(LegacyParticipantFactory.class.getName()));
    }

    private static List<String> reflectiveTypes(IndexView index) {
        List<String> types = new ArrayList<>(List.of(
                CallbackAddress.class.getName(),
                ParticipantRequest.class.getName(),
                ParticipantReply.class.getName(),
                ParticipantCallbacks.class.getName(),
                CallbackType.class.getName(),
                LRA.ParticipantOutcome.class.getName(),
                LRA.TransactionStatus.class.getName(),
                LRA.Transaction.class.getName(),
                LRA.CancelOn.class.getName(),
                LRA.Complete.class.getName(),
                LRA.Compensate.class.getName(),
                LRA.Status.class.getName(),
                LRA.Forget.class.getName(),
                LRA.AfterLRA.class.getName(),
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
}
