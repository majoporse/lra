package io.narayana.lra.coordinator.domain.model.actions;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = HttpAction.class, name = "http"),
        @JsonSubTypes.Type(value = KafkaAction.class, name = "kafka")
})
public interface LRAAction {

    ActionResult call(ActionContext context);

    /**
     * Extract the LRA UID from the action target if it points to a local/nested LRA.
     * Returns null if the action does not target a known LRA.
     */
    String extractTargetUid();

    String toJson();

    static LRAAction fromJson(String json) {
        if (json == null)
            return null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(json, LRAAction.class);
        } catch (Exception e) {
            // not valid JSON — treat as raw URI (legacy format)
            return new HttpAction(json);
        }
    }
}