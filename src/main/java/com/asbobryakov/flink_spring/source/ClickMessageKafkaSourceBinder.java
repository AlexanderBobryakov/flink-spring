package com.asbobryakov.flink_spring.source;

import com.asbobryakov.flink_spring.properties.KafkaProperties;
import com.asbobryakov.flink_spring.schema.ClickMessage;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Component;

import java.time.Duration;

import lombok.RequiredArgsConstructor;

import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;

@Component
@RequiredArgsConstructor
class ClickMessageKafkaSourceBinder implements SourceBinder<ClickMessage> {
    private final KafkaProperties kafkaProperties;
    private final DeserializationSchema<ClickMessage> deserializationClickMessageSchema;

    @Override
    public DataStream<ClickMessage> bindSource(StreamExecutionEnvironment environment) {
        final String sourceName = "Click message Kafka source";
        return environment.fromSource(
            KafkaSource.<ClickMessage>builder()
                .setBootstrapServers(kafkaProperties.getBootstrapServers())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(EARLIEST))
                .setGroupId(kafkaProperties.getGroupId())
                .setTopics(kafkaProperties.getTopics().getClickTopic())
                .setValueOnlyDeserializer(deserializationClickMessageSchema)
                .build(),
            WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)),
            sourceName
        ).name(sourceName).uid(sourceName + "_id");
    }
}
