package com.asbobryakov.flink_spring.source.deserialization;

import com.asbobryakov.flink_spring.schema.ClickMessage;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.springframework.stereotype.Component;

import java.io.IOException;

import lombok.RequiredArgsConstructor;

import static com.asbobryakov.flink_spring.config.ObjectMapperConfig.createObjectMapper;


@Component
@RequiredArgsConstructor
class ClickMessageDeserializationSchema implements DeserializationSchema<ClickMessage> {
    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = createObjectMapper();
    }

    @Override
    public ClickMessage deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, ClickMessage.class);
    }

    @Override
    public boolean isEndOfStream(ClickMessage nextElement) {
        return false;
    }

    @Override
    public TypeInformation<ClickMessage> getProducedType() {
        return TypeInformation.of(ClickMessage.class);
    }
}