package com.asbobryakov.flink_spring.schema.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

import java.util.UUID;

@Data
@Builder
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class AlertMessage {
    @JsonPropertyDescription("User id")
    private UUID userId;

    @JsonPropertyDescription("Trigger name")
    private String triggerName;

    @JsonPropertyDescription("Timestamp")
    private Long timestamp;
}
