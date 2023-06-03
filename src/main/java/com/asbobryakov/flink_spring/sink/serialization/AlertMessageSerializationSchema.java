package com.asbobryakov.flink_spring.sink.serialization;

import com.asbobryakov.flink_spring.schema.kafka.AlertMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.springframework.stereotype.Component;

import static com.asbobryakov.flink_spring.config.ObjectMapperConfig.createObjectMapper;

@Component
@RequiredArgsConstructor
class AlertMessageSerializationSchema implements SerializationSchema<AlertMessage> {
    private static final long serialVersionUID = 1;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = createObjectMapper();
    }

    @Override
    @SneakyThrows
    public byte[] serialize(AlertMessage element) {
        return objectMapper.writeValueAsBytes(element);
    }
}
