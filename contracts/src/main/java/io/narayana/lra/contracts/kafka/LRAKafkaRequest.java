package io.narayana.lra.contracts.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public interface LRAKafkaRequest {
    String getCorrelationId();

    String getReplyTopic();
}
