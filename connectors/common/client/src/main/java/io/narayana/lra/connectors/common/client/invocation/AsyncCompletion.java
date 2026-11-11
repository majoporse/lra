/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.invocation;

/**
 * The handoff through which a transport reports the eventual result of an asynchronous invocation.
 * <p>
 * It carries the transport's raw value and failure, not a {@link LraCompletion}: the interceptor
 * owns the mapping through {@code LRATransport.completionOf}, so a transport adapter never decides
 * LRA state.
 */
@FunctionalInterface
public interface AsyncCompletion {

    /**
     * @param value the value the invocation produced, or {@code null} when it failed
     * @param thrown the failure the invocation produced, or {@code null} when it succeeded
     */
    void completed(Object value, Throwable thrown);
}
