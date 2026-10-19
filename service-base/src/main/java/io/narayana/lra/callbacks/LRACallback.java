package io.narayana.lra.callbacks;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = LegacyHttpCallback.class, name = "http"),
        @JsonSubTypes.Type(value = HttpCallback.class, name = HttpCallback.TYPE),
        @JsonSubTypes.Type(value = KafkaCallback.class, name = "kafka")
})
public interface LRACallback {

    CallbackResult call(CallbackContext context);

    String toJson();

    static String callbackUri(LRACallback callback) {
        if (callback instanceof HttpCallback http) {
            return http.getUri();
        }

        if (callback instanceof LegacyHttpCallback http) {
            return http.getUri();
        }

        return callback != null ? callback.toJson() : null;
    }

    static LRACallback fromJson(String json) throws JsonProcessingException {
        if (json == null)
            return null;
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(json, LRACallback.class);
    }
}
