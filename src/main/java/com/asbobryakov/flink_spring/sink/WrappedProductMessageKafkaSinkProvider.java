package com.asbobryakov.flink_spring.sink;

import com.asbobryakov.flink_spring.properties.KafkaProperties;
import com.asbobryakov.flink_spring.schema.ProductMessage;
import com.asbobryakov.flink_spring.schema.WrappedSinkMessage;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

import static org.apache.flink.connector.base.DeliveryGuarantee.NONE;

@Component
@RequiredArgsConstructor
public class WrappedProductMessageKafkaSinkProvider implements SinkProvider<WrappedSinkMessage<ProductMessage>> {
    private final KafkaProperties kafkaProperties;
    private final SerializationSchema<WrappedSinkMessage<ProductMessage>> serializationProductMessageSchema;

    @Override
    public Sink<WrappedSinkMessage<ProductMessage>> createSink() {
        return KafkaSink.<WrappedSinkMessage<ProductMessage>>builder()
                   .setBootstrapServers(kafkaProperties.getBootstrapServers())
                   .setRecordSerializer(KafkaRecordSerializationSchema.<WrappedSinkMessage<ProductMessage>>builder()
                                            .setTopicSelector(wrappedMessage -> wrappedMessage.getMeta().getTargetTopicName())
                                            .setValueSerializationSchema(serializationProductMessageSchema)
                                            .build())
                   .setDeliveryGuarantee(NONE)
                   .build();
    }
}
