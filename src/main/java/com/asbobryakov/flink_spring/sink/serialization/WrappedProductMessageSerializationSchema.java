package com.asbobryakov.flink_spring.sink.serialization;

import com.asbobryakov.flink_spring.schema.ProductMessage;
import com.asbobryakov.flink_spring.schema.WrappedSinkMessage;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import static com.asbobryakov.flink_spring.config.ObjectMapperConfig.createObjectMapper;

@Component
@RequiredArgsConstructor
class WrappedProductMessageSerializationSchema implements SerializationSchema<WrappedSinkMessage<ProductMessage>> {
    private static final long serialVersionUID = 1;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = createObjectMapper();
    }

    @Override
    @SneakyThrows
    public byte[] serialize(WrappedSinkMessage<ProductMessage> element) {
        return objectMapper.writeValueAsBytes(element.getMessage());
    }
}
