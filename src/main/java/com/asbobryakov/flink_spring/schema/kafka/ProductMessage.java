package com.asbobryakov.flink_spring.schema.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

import java.util.UUID;

@Data
@Builder
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductMessage {
    @JsonPropertyDescription("User id")
    private UUID userId;

    @JsonPropertyDescription("Product name")
    private String productName;

    @JsonPropertyDescription("Clicked object")
    private String object;

    @JsonPropertyDescription("User id")
    @JsonDeserialize(using = Platform.Deserializer.class)
    private Platform platform;

    @JsonPropertyDescription("Timestamp")
    private Long timestamp;
}