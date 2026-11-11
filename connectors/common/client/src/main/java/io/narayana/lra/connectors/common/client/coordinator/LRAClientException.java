/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.coordinator;

/**
 * A coordinator call that failed, in the vocabulary every connector can report.
 * <p>
 * The lifecycle processor is transport-neutral, so it cannot catch the JAX-RS exceptions the HTTP
 * client raises. Each client translates its own transport failures into this type; the status code
 * carries the value an HTTP adapter would have used so a transport can map it back to its own error.
 * When the transport returned a body it is kept, because callers of the client use it to diagnose
 * the failure.
 */
public class LRAClientException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public enum Kind {
        /** the coordinator does not know the LRA (for example it has already ended) */
        NOT_FOUND,
        /** the coordinator could not be reached or is temporarily unable to serve the request */
        UNAVAILABLE,
        /** any other coordinator failure */
        OTHER
    }

    private final int statusCode;
    private final Kind kind;
    private final String responseBody;

    public LRAClientException(int statusCode, Kind kind, String message) {
        this(statusCode, kind, message, null, null);
    }

    public LRAClientException(int statusCode, Kind kind, String message, Throwable cause) {
        this(statusCode, kind, message, null, cause);
    }

    public LRAClientException(int statusCode, Kind kind, String message, String responseBody, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.kind = kind;
        this.responseBody = responseBody;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public Kind getKind() {
        return kind;
    }

    /**
     * @return the body the transport returned, or {@code null} when there was none
     */
    public String getResponseBody() {
        return responseBody;
    }
}
