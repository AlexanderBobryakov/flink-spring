package com.asbobryakov.flink_spring.source.deserialization;

import com.asbobryakov.flink_spring.schema.kafka.TriggerMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.springframework.stereotype.Component;

import java.io.IOException;

import static com.asbobryakov.flink_spring.config.ObjectMapperConfig.createObjectMapper;


@Component
@RequiredArgsConstructor
class TriggerMessageDeserializationSchema implements DeserializationSchema<TriggerMessage> {
    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = createObjectMapper();
    }

    @Override
    public TriggerMessage deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, TriggerMessage.class);
    }

    @Override
    public boolean isEndOfStream(TriggerMessage nextElement) {
        return false;
    }

    @Override
    public TypeInformation<TriggerMessage> getProducedType() {
        return TypeInformation.of(TriggerMessage.class);
    }
}
