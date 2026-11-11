/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.address;

import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * Registers the HTTP callback address subtypes with Jackson.
 * <p>
 * {@code ObjectMapper.findAndRegisterModules()} discovers this module through
 * {@code META-INF/services/com.fasterxml.jackson.databind.Module}, so the coordinator and the
 * participant can read HTTP addresses without a central subtype list.
 */
public final class HttpCallbackAddressModule extends SimpleModule {

    public HttpCallbackAddressModule() {
        super("lra-http-callback-address");
        registerSubtypes(HttpContractAddress.class, LegacyHttpAddress.class);
    }
}
