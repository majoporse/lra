/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CoordinatorConfigTest {

    @Test
    public void usesTheFirstConfiguredUrlWithoutATrailingSlash() {
        String previous = System.getProperty(CoordinatorConfig.COORDINATOR_URL_PROPERTY);
        System.setProperty(CoordinatorConfig.COORDINATOR_URL_PROPERTY,
                "http://coordinator:8080/lra-coordinator/,http://other:8080/lra-coordinator");

        try {
            CoordinatorConfig config = new CoordinatorConfig();

            Assertions.assertEquals("http://coordinator:8080/lra-coordinator", config.baseUrl());
            Assertions.assertEquals("http://coordinator:8080/lra-coordinator/recovery",
                    config.recoveryUrlBase());
        } finally {
            restore(previous);
        }
    }

    @Test
    public void defaultsToTheLocalCoordinator() {
        String previous = System.getProperty(CoordinatorConfig.COORDINATOR_URL_PROPERTY);
        System.clearProperty(CoordinatorConfig.COORDINATOR_URL_PROPERTY);

        try {
            CoordinatorConfig config = new CoordinatorConfig();

            Assertions.assertEquals("http://localhost:8080/lra-coordinator", config.baseUrl());
        } finally {
            restore(previous);
        }
    }

    private static void restore(String previous) {
        if (previous == null) {
            System.clearProperty(CoordinatorConfig.COORDINATOR_URL_PROPERTY);
        } else {
            System.setProperty(CoordinatorConfig.COORDINATOR_URL_PROPERTY, previous);
        }
    }
}
