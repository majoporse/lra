/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client;

import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.inject.Qualifier;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Identifies the connector that supplies a client or an endpoint resolver.
 * <p>
 * The qualifier exists so the registry can select a connector without instantiating every
 * implementation: a Kafka client may need configuration that is absent when the participant
 * protocol is HTTP.
 */
@Qualifier
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER })
public @interface ParticipantConnector {

    String value();

    final class Literal extends AnnotationLiteral<ParticipantConnector> implements ParticipantConnector {

        private final String value;

        private Literal(String value) {
            this.value = value;
        }

        public static Literal of(String value) {
            return new Literal(value);
        }

        @Override
        public String value() {
            return value;
        }
    }
}
