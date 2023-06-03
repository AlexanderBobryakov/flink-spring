package com.asbobryakov.flink_spring.testutils.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.asbobryakov.flink_spring.config.ObjectMapperConfig.createObjectMapper;
import static java.util.stream.Collectors.toSet;

@TestComponent
@SuppressWarnings("PMD.TestClassWithoutTestCases")
public class TestKafkaFacade {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private KafkaAdmin kafkaAdmin;

    private final ObjectMapper objectMapper = createObjectMapper();

    public void createTopicsIfNeeded(String... names) {
        final var topics = kafkaAdmin.describeTopics();
        if (!topics.keySet().containsAll(Stream.of(names).collect(toSet()))) {
            kafkaAdmin.createOrModifyTopics(
                Stream.of(names)
                    .map(n -> new NewTopic(n, 1, (short) 1))
                    .toArray(NewTopic[]::new)
            );
        }
    }

    @SneakyThrows
    public void sendMessage(String topic, Object message) {
        kafkaTemplate
            .send(topic, objectMapper.writeValueAsString(message))
            .get(5, TimeUnit.SECONDS);
    }

    public KafkaTestConsumer createKafkaConsumer(Set<String> topics) {
        return new KafkaTestConsumer(System.getProperty("kafka.bootstrap-servers"), topics);
    }
}
