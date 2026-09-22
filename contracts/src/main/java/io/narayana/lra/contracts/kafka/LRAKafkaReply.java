package io.narayana.lra.contracts.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public interface LRAKafkaReply {
    String getCorrelationId();

    String getError();
}
