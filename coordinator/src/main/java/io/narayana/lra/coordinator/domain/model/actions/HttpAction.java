package io.narayana.lra.coordinator.domain.model.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;

public class HttpAction implements LRAAction {
    public enum HttpMethod {
        PUT,
        GET,
        DELETE
    }

    public enum ContextType {
        ACTIVE,
        ENDED
    }

    private final String uri;
    private final HttpMethod method;
    private final ContextType contextType;

    @JsonCreator
    public HttpAction(
            @JsonProperty("uri") String uri,
            @JsonProperty("method") HttpMethod method,
            @JsonProperty("contextType") ContextType contextType) {
        this.uri = uri;
        this.method = method != null ? method : HttpMethod.PUT;
        this.contextType = contextType != null ? contextType : ContextType.ACTIVE;
    }

    public HttpAction(String uri) {
        this(uri, HttpMethod.PUT, ContextType.ACTIVE);
    }

    public HttpAction(URI uri) {
        this(uri.toASCIIString(), HttpMethod.PUT, ContextType.ACTIVE);
    }

    public HttpAction(URI uri, HttpMethod method, ContextType contextType) {
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
    public ActionResult call(ActionContext context) {
        Client client = null;
        try {
            client = jakarta.ws.rs.client.ClientBuilder.newClient();
            var builder = client.target(uri)
                    .request()
                    .header(LRA.LRA_HTTP_RECOVERY_HEADER, context.getRecoveryId())
                    .header("Narayana-LRA-Participant-Data", context.getCompensatorData());

            if (contextType == ContextType.ENDED) {
                builder.header(LRA.LRA_HTTP_ENDED_CONTEXT_HEADER, context.getLraId());
                if (context.getParentId() != null) {
                    builder.header(LRA.LRA_HTTP_PARENT_CONTEXT_HEADER, context.getParentId());
                }
            } else {
                builder.header(LRA.LRA_HTTP_CONTEXT_HEADER, context.getLraId());
                if (context.getParentId() != null) {
                    builder.header(LRA.LRA_HTTP_PARENT_CONTEXT_HEADER, context.getParentId());
                }
            }

            Response response;
            switch (method) {
                case GET:
                    response = builder.async().get().get(30, TimeUnit.SECONDS);
                    break;
                case DELETE:
                    response = builder.async().delete().get(30, TimeUnit.SECONDS);
                    break;
                case PUT:
                default:
                    String payload = context.getPayload() != null ? context.getPayload() : "";
                    response = builder.async().put(Entity.text(payload)).get(30, TimeUnit.SECONDS);
                    break;
            }

            int status = response.getStatus();
            ActionStatus actionStatus = mapStatus(status);
            String body = response.hasEntity() ? response.readEntity(String.class) : null;
            LRAAction updatedStatusAction = null;

            if (actionStatus == ActionStatus.ACCEPTED) {
                String location = response.getHeaderString("Location");
                if (location != null) {
                    updatedStatusAction = new HttpAction(location, HttpMethod.GET, ContextType.ACTIVE);
                }
            }

            return new ActionResult(actionStatus, body, updatedStatusAction);
        } catch (Exception e) {
            return new ActionResult(ActionStatus.TIMEOUT);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    private static ActionStatus mapStatus(int httpStatus) {
        if (httpStatus >= 200 && httpStatus < 300) {
            return httpStatus == 202 ? ActionStatus.ACCEPTED : ActionStatus.OK;
        }
        if (httpStatus == 410) {
            return ActionStatus.GONE;
        }
        return ActionStatus.ERROR;
    }

    @Override
    public String extractTargetUid() {
        try {
            java.net.URI parsed = new java.net.URI(uri);
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
            throw new RuntimeException("Failed to serialize HttpAction", e);
        }
    }
}