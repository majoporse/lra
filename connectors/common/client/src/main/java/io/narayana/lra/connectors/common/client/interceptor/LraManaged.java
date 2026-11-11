/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.interceptor;

import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.interceptor.InterceptorBinding;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Internal marker that routes a method to {@link LRAInterceptor}.
 * <p>
 * It carries no LRA semantics and is never read at runtime. The portable CDI extension and the
 * Quarkus build step add it to the CDI metadata of every method the interceptor is responsible for
 * ({@code @LRA}, {@code @Leave} and method-level {@code @PropagateToken}), because {@code @Leave}
 * and {@code @PropagateToken} are not interceptor bindings themselves.
 * <p>
 * Using a dedicated marker instead of synthesising {@code @LRA} keeps the binding inert: reading a
 * synthesized {@code @LRA} would make a {@code @Leave}-only method start and end an LRA. The
 * interceptor still reads the real annotations through
 * {@link io.narayana.lra.connectors.common.client.invocation.LraMethodMetadata}.
 */
@InterceptorBinding
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface LraManaged {

    /**
     * Annotation literal for the CDI extension, which adds the marker through an
     * {@code AnnotatedMethodConfigurator}.
     */
    final class Literal extends AnnotationLiteral<LraManaged> implements LraManaged {

        private static final long serialVersionUID = 1L;

        public static final Literal INSTANCE = new Literal();

        private Literal() {
        }
    }
}
