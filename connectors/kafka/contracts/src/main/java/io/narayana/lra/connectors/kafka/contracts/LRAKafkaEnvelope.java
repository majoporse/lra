package io.narayana.lra.connectors.kafka.contracts;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class LRAKafkaEnvelope {
    public String type;
    public Object payload;

    public LRAKafkaEnvelope() {
    }

    public LRAKafkaEnvelope(String type, Object payload) {
        this.type = type;
        this.payload = payload;
    }
}
