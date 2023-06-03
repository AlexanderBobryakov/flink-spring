package com.asbobryakov.flink_spring.properties;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@ConfigurationProperties("kafka")
@ConstructorBinding
@RequiredArgsConstructor
@Getter
public class KafkaProperties {
    @NotNull(message = "Kafka group-id cannot be null")
    private final String groupId;
    @NotNull(message = "Kafka bootstrap servers cannot be null")
    private final String bootstrapServers;
    @NotNull(message = "Topics cannot be null")
    private final Topics topics;

    @ConfigurationProperties(prefix = "topics")
    @ConstructorBinding
    @RequiredArgsConstructor
    @Getter
    public static class Topics {
        @NotEmpty(message = "Click Topic cannot be null or empty")
        private final String clickTopic;
        @NotEmpty(message = "Trigger Topic cannot be null or empty")
        private final String triggerTopic;
        @NotEmpty(message = "Alert Topic cannot be null or empty")
        private final String alertTopic;
    }
}
