package com.asbobryakov.flink_spring.sink;

import com.asbobryakov.flink_spring.properties.KafkaProperties;
import com.asbobryakov.flink_spring.schema.kafka.AlertMessage;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.springframework.stereotype.Component;

import static org.apache.flink.connector.base.DeliveryGuarantee.NONE;

@Component
@RequiredArgsConstructor
public class AlertMessageKafkaSinkProvider implements SinkProvider<AlertMessage> {
    private final KafkaProperties kafkaProperties;
    private final SerializationSchema<AlertMessage> serializationProductMessageSchema;

    @Override
    public Sink<AlertMessage> createSink() {
        return KafkaSink.<AlertMessage>builder()
                .setBootstrapServers(kafkaProperties.getBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.<AlertMessage>builder()
                        .setTopic(kafkaProperties.getTopics().getAlertTopic())
                        .setValueSerializationSchema(serializationProductMessageSchema)
                        .build())
                .setDeliveryGuarantee(NONE)
                .build();
    }
}
