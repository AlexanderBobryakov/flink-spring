package com.asbobryakov.flink_spring.testutils.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import lombok.SneakyThrows;

import static com.asbobryakov.flink_spring.config.ObjectMapperConfig.createObjectMapper;
import static java.util.stream.Collectors.toList;

public class KafkaTestConsumer implements AutoCloseable {
    private final Consumer<String, String> consumer;
    private final List<KafkaMessage> receivedMessages = new CopyOnWriteArrayList<>();
    private final ObjectMapper objectMapper = createObjectMapper();

    public KafkaTestConsumer(String bootstrapServers, Set<String> topics) {
        this.consumer = new KafkaConsumer<>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
            )
        );
        consumer.subscribe(topics);
    }

    public <T> List<T> receiveAndGetAll(String topic, Class<T> clazz) {
        return receiveAndGetAll()
                   .stream()
                   .filter(kafkaMessage -> topic.equals(kafkaMessage.getTopic()))
                   .map(kafkaMessage -> readValue(kafkaMessage, clazz))
                   .collect(toList());
    }

    private List<KafkaMessage> receiveAndGetAll() {
        final var records = consumer.poll(Duration.ofSeconds(5));
        for (ConsumerRecord<String, String> record : records) {
            receivedMessages.add(new KafkaMessage(record.key(), record.topic(), record.value()));
        }
        consumer.commitSync();
        return receivedMessages;
    }

    @SneakyThrows
    private <T> T readValue(KafkaMessage kafkaMessage, Class<T> clazz) {
        return objectMapper.readValue(kafkaMessage.getValue(), clazz);
    }

    @Override
    public void close() {
        receivedMessages.clear();
        consumer.close();
    }
}
