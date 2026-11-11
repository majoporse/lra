/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.config;

import io.narayana.lra.LRAConstants;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * The coordinator's public address, read only from configuration.
 */
public class CoordinatorConfig {

    public static final String COORDINATOR_URL_PROPERTY = "lra.coordinator.url";

    private static final String DEFAULT_URL = "http://localhost:8080/" + LRAConstants.COORDINATOR_PATH_NAME;

    public String baseUrl() {
        String base = ConfigProvider.getConfig()
                .getOptionalValue(COORDINATOR_URL_PROPERTY, String.class)
                .orElse(DEFAULT_URL);

        int comma = base.indexOf(',');

        if (comma >= 0) {
            base = base.substring(0, comma);
        }

        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }

        return base;
    }

    public String recoveryUrlBase() {
        return baseUrl() + "/" + LRAConstants.RECOVERY_COORDINATOR_PATH_NAME;
    }
}
