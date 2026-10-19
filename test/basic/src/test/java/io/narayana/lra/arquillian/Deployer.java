/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.arquillian;

import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;

public class Deployer {

    /**
     * Deploys the application with the legacy default configuration: the JAX-RS
     * {@code ServerLRAFilter} owns the LRA lifecycle.
     */
    public static WebArchive deploy(String appName, Class... participants) {
        return deploy(appName, null, participants);
    }

    /**
     * Deploys the application in the CDI interceptor mode: the participant container runs with
     * {@code lra.server.filter.interceptor=true} and requires the participant callbacks base URI.
     *
     * @param appName the name of the deployed application (also its HTTP context path)
     * @param coordinatorBaseUri the base URI the LRA coordinator can use to call the participant
     *        application back, ie the value of {@code narayana.lra.base-uri}; when {@code null} the
     *        default (request derived) handling is kept and only {@code lra.server.filter.interceptor}
     *        is switched on
     */
    public static WebArchive deployBeanParticipantApp(String appName, String coordinatorBaseUri, Class... participants) {
        StringBuilder config = new StringBuilder("lra.server.filter.interceptor=true\n");

        if (coordinatorBaseUri != null) {
            config.append("narayana.lra.base-uri=").append(coordinatorBaseUri).append('\n');
        }

        WebArchive archive = deploy(appName, config.toString(), participants);

        // register the LRA CDI extensions so that the participant registries are populated
        // (the proxy/api service file is not a class and is not added by addPackages)
        archive.addAsResource("META-INF/services/jakarta.enterprise.inject.spi.Extension");

        // the registries are registered by the extensions themselves (with the populated
        // participant maps), so use annotated bean discovery: were a registry discovered as an
        // implicit @Default managed bean (bean-discovery-mode="all"), the injection would be
        // ambiguous
        archive.delete("WEB-INF/beans.xml");
        archive.addAsWebInfResource(new StringAsset("<beans version=\"1.1\" bean-discovery-mode=\"annotated\"></beans>"),
                "beans.xml");

        return archive;
    }

    private static WebArchive deploy(String appName, String lraConfig, Class... participants) {
        // manifest for WildFly deployment; org.jboss.modules is needed by the CDI extension
        // class path scan (ClassPathIndexer), org.jboss.jandex for the Jandex index
        final String ManifestMF = "Manifest-Version: 1.0\n"
                + "Dependencies: org.jboss.jandex, org.jboss.logging, org.jboss.modules\n";

        WebArchive archive = ShrinkWrap.create(WebArchive.class, appName + ".war")
                .addPackages(true,
                        "org.eclipse.microprofile.lra",
                        "io.narayana.lra.client.internal.proxy",
                        "io.narayana.lra.contracts",
                        "io.narayana.lra.callbacks.contracts",
                        "io.smallrye.stork",
                        "io.narayana.lra.filter",
                        "io.smallrye.mutiny") // try to pull in everything under mutiny)
                .addPackages(false,
                        "io.narayana.lra",
                        "io.narayana.lra.api",
                        "io.narayana.lra.callbacks",
                        "io.narayana.lra.logging",
                        "io.narayana.lra.provider",
                        "io.narayana.lra.context",
                        "io.narayana.lra.client",
                        "io.narayana.lra.client.internal",
                        "io.narayana.lra.arquillian.spi",
                        "io.smallrye.stork",
                        "io.smallrye.mutiny", // recursive addPacakges doesn't seem to resolve the missing classes, so:
                        "io.smallrye.mutiny.helpers",
                        "io.smallrye.mutiny.operators.multi",
                        "io.smallrye.mutiny.subscription",
                        "io.smallrye.mutiny.helpers.spies",
                        "io.smallrye.mutiny.helpers.test")

                // adds the TestBase class, the test class itself seems to be uploaded during deployment by Arquillian
                // then it requires the parent class as well, otherwise Weld NoClassDefFoundError is shown
                .addClass(TestBase.class)

                // adds the lra-participant wanted
                .addClasses(participants)

                // activates Wildfly modules
                .addAsManifestResource(new StringAsset(ManifestMF), "MANIFEST.MF")

                // activates the bean and explicitly specifies to work with annotated classes
                .addAsWebInfResource(new StringAsset("<beans version=\"1.1\" bean-discovery-mode=\"all\"></beans>"),
                        "beans.xml")

                // services from narayana-lra.jar
                .addAsResource(new StringAsset("org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder"),
                        "META-INF/services/jakarta.ws.rs.client.ClientBuilder")
                .addAsResource(new StringAsset("io.narayana.lra.context.ClientLRAContextProviderEE"),
                        "META-INF/services/jakarta.enterprise.concurrent.spi.ThreadContextProvider");

        if (lraConfig != null) {
            archive.addAsResource(new StringAsset(lraConfig),
                    "META-INF/microprofile-config.properties");
        }

        return archive;
    }
}
