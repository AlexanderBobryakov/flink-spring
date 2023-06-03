package com.asbobryakov.flink_spring.schema.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
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
public class ClickMessage {
    @JsonProperty(required = true)
    @JsonPropertyDescription("User id")
    private UUID userId;

    @JsonPropertyDescription("Clicked object")
    private String object;

    @JsonPropertyDescription("User platform")
    @JsonDeserialize(using = Platform.Deserializer.class)
    private Platform platform;

    @JsonPropertyDescription("Product name")
    private String productName;

    @JsonPropertyDescription("Product topic")
    private String productTopic;

    @JsonProperty(required = true)
    @JsonPropertyDescription("Timestamp")
    private Long timestamp;

    @JsonPropertyDescription("Additional data")
    private Map<String, Object> data;
}

