package com.asbobryakov.flink_spring.source;

import com.asbobryakov.flink_spring.properties.KafkaProperties;
import com.asbobryakov.flink_spring.schema.kafka.TriggerMessage;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Component;

import java.time.Duration;

import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;

@Component
@RequiredArgsConstructor
class TriggerMessageKafkaSourceBinder implements SourceBinder<TriggerMessage> {
    private final KafkaProperties kafkaProperties;
    private final DeserializationSchema<TriggerMessage> deserializationClickMessageSchema;

    @Override
    public DataStream<TriggerMessage> bindSource(StreamExecutionEnvironment environment) {
        final var sourceName = "Trigger message Kafka source";
        return environment.fromSource(
                KafkaSource.<TriggerMessage>builder()
                        .setBootstrapServers(kafkaProperties.getBootstrapServers())
                        .setStartingOffsets(OffsetsInitializer.committedOffsets(EARLIEST))
                        .setGroupId(kafkaProperties.getGroupId())
                        .setTopics(kafkaProperties.getTopics().getTriggerTopic())
                        .setValueOnlyDeserializer(deserializationClickMessageSchema)
                        .build(),
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)),
                sourceName
        ).name(sourceName).uid(sourceName + "_id");
    }
}
