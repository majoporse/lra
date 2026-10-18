/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.callbacks;

/**
 * The protocol selected by {@code quarkus.lra.client.protocol}. The same protocol is used for the
 * client's coordinator transport and for its participant callbacks; they are never mixed.
 */
public enum Protocol {
    HTTP,
    KAFKA;

    /**
     * The configuration values. These are compile-time constants so that they can be used in
     * annotations such as {@code @IfBuildProperty}, where a call to {@link #value()} is not a
     * constant expression.
     */
    public static final String HTTP_VALUE = "http";
    public static final String KAFKA_VALUE = "kafka";

    /**
     * The value users write in configuration.
     */
    public String value() {
        return switch (this) {
            case HTTP -> HTTP_VALUE;
            case KAFKA -> KAFKA_VALUE;
        };
    }

    /**
     * Resolves a configured value.
     *
     * @param value the configured value, case-insensitive and optionally padded with whitespace
     * @return the matching protocol
     * @throws IllegalArgumentException if the value is not a known protocol
     */
    public static Protocol from(String value) {
        if (value != null) {
            String trimmed = value.trim();

            for (Protocol protocol : values()) {
                if (protocol.value().equalsIgnoreCase(trimmed)) {
                    return protocol;
                }
            }
        }

        throw new IllegalArgumentException("Unsupported protocol '" + value + "'. Supported values are '"
                + HTTP_VALUE + "' and '" + KAFKA_VALUE + "'.");
    }
}
