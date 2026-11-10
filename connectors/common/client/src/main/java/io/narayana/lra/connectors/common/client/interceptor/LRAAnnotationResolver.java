/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.interceptor;

import io.narayana.lra.connectors.common.client.invocation.LraMethodMetadata;
import java.lang.reflect.Method;

/**
 * Abstracts the source of the LRA annotations so that the lifecycle processor can be driven by the
 * MicroProfile LRA annotations. The implementation that reads those annotations lives with the
 * annotation artifact, because they carry JAX-RS types.
 */
public interface LRAAnnotationResolver {

    /**
     * Resolve all method-level LRA declarations once.
     *
     * @param method the method being processed
     * @return the declared metadata; never {@code null}
     */
    LraMethodMetadata resolve(Method method);
}
