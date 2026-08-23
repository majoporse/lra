package io.narayana.lra.contracts.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class KafkaActionBody {
    public final String lraId;
    public final String recoveryId;
    public final String compensatorData;

    @JsonCreator
    public KafkaActionBody(
            @JsonProperty("lraId") String lraId,
            @JsonProperty("recoveryId") String recoveryId,
            @JsonProperty("compensatorData") String compensatorData) {
        this.lraId = lraId;
        this.recoveryId = recoveryId;
        this.compensatorData = compensatorData;
    }
}
