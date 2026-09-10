package io.narayana.lra.callbacks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.narayana.lra.LRAConstants;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;

public class HttpCallback implements LRACallback {
    public enum HttpMethod {
        PUT,
        GET,
        DELETE
    }

    public enum ContextType {
        ACTIVE,
        ENDED
    }

    @JsonProperty("type")
    public String getType() {
        return "http";
    }

    private final String uri;
    private final HttpMethod method;
    private final ContextType contextType;

    /**
     * Factory for a compensate (@Compensate) callback: PUT with the ACTIVE LRA context.
     */
    public static HttpCallback compensateCallback(URI uri) {
        return new HttpCallback(uri, HttpMethod.PUT, ContextType.ACTIVE);
    }

    /**
     * Factory for a complete (@Complete) callback: PUT with the ACTIVE LRA context.
     */
    public static HttpCallback completeCallback(URI uri) {
        return new HttpCallback(uri, HttpMethod.PUT, ContextType.ACTIVE);
    }

    /**
     * Factory for a status (@Status) callback: GET with the ACTIVE LRA context.
     */
    public static HttpCallback statusCallback(URI uri) {
        return new HttpCallback(uri, HttpMethod.GET, ContextType.ACTIVE);
    }

    /**
     * Factory for a forget (@Forget) callback: DELETE with the ACTIVE LRA context.
     */
    public static HttpCallback forgetCallback(URI uri) {
        return new HttpCallback(uri, HttpMethod.DELETE, ContextType.ACTIVE);
    }

    /**
     * Factory for an after LRA (@AfterLRA) notification callback: PUT with the ENDED LRA context.
     */
    public static HttpCallback afterCallback(URI uri) {
        return new HttpCallback(uri, HttpMethod.PUT, ContextType.ENDED);
    }

    // NOTE: construction is only possible via the static factories or Jackson deserialization;
    // this ensures the HTTP method and context type always match the endpoint semantics.

    @JsonCreator
    private HttpCallback(
            @JsonProperty("uri") String uri,
            @JsonProperty("method") HttpMethod method,
            @JsonProperty("contextType") ContextType contextType) {
        this.uri = uri;
        this.method = method != null ? method : HttpMethod.PUT;
        this.contextType = contextType != null ? contextType : ContextType.ACTIVE;
    }

    private HttpCallback(URI uri, HttpMethod method, ContextType contextType) {
        this(uri.toASCIIString(), method, contextType);
    }

    public String getUri() {
        return uri;
    }

    public HttpMethod getMethod() {
        return method;
    }

    public ContextType getContextType() {
        return contextType;
    }

    @Override
    public CallbackResult call(CallbackContext context) {
        Client client = null;
        try {
            client = jakarta.ws.rs.client.ClientBuilder.newClient();
            var builder = client.target(uri)
                    .request()
                    .header(LRA.LRA_HTTP_RECOVERY_HEADER, context.getRecoveryId())
                    .header("Narayana-LRA-Participant-Data", context.getCompensatorData());
            if (context.getParentId() != null) {
                builder.header(LRA.LRA_HTTP_PARENT_CONTEXT_HEADER, context.getParentId());
            }

            if (contextType == ContextType.ENDED) {
                builder.header(LRA.LRA_HTTP_ENDED_CONTEXT_HEADER, context.getLraId());
            } else {
                builder.header(LRA.LRA_HTTP_CONTEXT_HEADER, context.getLraId());
            }

            Response response = switch (method) {
                case GET -> builder.async().get().get(LRAConstants.PARTICIPANT_TIMEOUT, TimeUnit.SECONDS);
                case DELETE -> builder.async().delete().get(LRAConstants.PARTICIPANT_TIMEOUT, TimeUnit.SECONDS);
                default -> {
                    String payload = context.getPayload() != null ? context.getPayload() : "";
                    yield builder.async().put(Entity.text(payload)).get(LRAConstants.PARTICIPANT_TIMEOUT,
                            TimeUnit.SECONDS);
                }
            };

            int status = response.getStatus();
            CallbackStatus callbackStatus = mapStatus(status);
            String body = response.hasEntity() ? response.readEntity(String.class) : null;
            LRACallback updatedStatusCallback = null;

            if (callbackStatus == CallbackStatus.ACCEPTED) {
                String location = response.getHeaderString("Location");
                if (location != null) {
                    updatedStatusCallback = statusCallback(URI.create(location));
                }
            }

            return new CallbackResult(callbackStatus, body, updatedStatusCallback);
        } catch (Exception e) {
            return new CallbackResult(CallbackStatus.TIMEOUT);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    private static CallbackStatus mapStatus(int httpStatus) {
        if (httpStatus >= 200 && httpStatus < 300)
            return httpStatus == 202 ? CallbackStatus.ACCEPTED : CallbackStatus.OK;
        if (httpStatus == 410)
            return CallbackStatus.GONE;
        if (httpStatus == 409)
            return CallbackStatus.FAILED;
        //        if (httpStatus == 500)
        //            return CallbackStatus.FAILED;
        return CallbackStatus.ERROR;
    }

    @Override
    public String extractTargetUid() {
        try {
            URI parsed = new URI(uri);
            String[] segments = parsed.getPath().split("/");
            int pCnt = segments.length;
            if (pCnt > 1) {
                return java.net.URLDecoder.decode(segments[pCnt - 2], java.nio.charset.StandardCharsets.UTF_8);
            }
        } catch (Exception ignore) {
        }
        return null;
    }

    @Override
    public String toJson() {
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize HttpCallback", e);
        }
    }
}
