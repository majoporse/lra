/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.support;

/**
 * The HTTP wire vocabulary shared by the HTTP connector modules.
 */
public final class HttpContractPaths {

    /**
     * The HTTP path of the common callback contract: {@code {base}/lra-callback-proxy/{participantId}/{op}}.
     */
    public static final String RESOURCE_PATH = "lra-callback-proxy";

    /**
     * The persisted discriminator of the HTTP contract address.
     */
    public static final String HTTP_CONTRACT = "http-contract";

    private HttpContractPaths() {
    }
}
