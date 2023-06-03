package com.asbobryakov.flink_spring.schema.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

import java.util.Map;
import java.util.UUID;

@Data
@Builder
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class TriggerMessage {
    @JsonPropertyDescription("User id")
    private UUID userId;

    @JsonPropertyDescription("Trigger name")
    private String triggerName;

    @JsonPropertyDescription("Trigger status")
    @JsonDeserialize(using = TriggerStatus.Deserializer.class)
    private TriggerStatus status;

    @JsonPropertyDescription("Device type")
    private String deviceType;

    @JsonPropertyDescription("Category")
    private String category;

    @JsonPropertyDescription("Trigger count")
    private int count;

    @JsonPropertyDescription("Timestamp")
    private Long timestamp;

    @JsonPropertyDescription("Trigger additional data")
    private Map<String, Object> data;
}
