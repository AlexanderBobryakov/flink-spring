package com.asbobryakov.flink_spring.sink;

import com.asbobryakov.flink_spring.properties.KafkaProperties;
import com.asbobryakov.flink_spring.schema.ProductMessage;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

import static org.apache.flink.connector.base.DeliveryGuarantee.NONE;

@Component
@RequiredArgsConstructor
public class ProductMessageKafkaSinkProvider implements SinkProvider<ProductMessage> {
    private final KafkaProperties kafkaProperties;
    private final SerializationSchema<ProductMessage> serializationProductMessageSchema;

    @Override
    public Sink<ProductMessage> createSink() {
        return KafkaSink.<ProductMessage>builder()
                   .setBootstrapServers(kafkaProperties.getBootstrapServers())
                   .setRecordSerializer(KafkaRecordSerializationSchema.<ProductMessage>builder()
                                            .setTopic(kafkaProperties.getTopics().getProductTopic())
                                            .setValueSerializationSchema(serializationProductMessageSchema)
                                            .build())
                   .setDeliveryGuarantee(NONE)
                   .build();
    }
}
