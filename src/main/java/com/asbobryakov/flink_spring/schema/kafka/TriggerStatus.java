package com.asbobryakov.flink_spring.schema.kafka;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import lombok.Value;

import java.io.IOException;
import java.util.Locale;

import static java.util.Objects.requireNonNullElse;

public interface TriggerStatus {
    String value();

    static TriggerStatus of(String value) {
        final String stringValue = requireNonNullElse(value, "");
        try {
            return Enum.parse(stringValue);
        } catch (IllegalArgumentException var3) {
            return new Simple(value);
        }
    }

    class Deserializer extends StdDeserializer<TriggerStatus> {
        private static final long serialVersionUID = 1L;

        protected Deserializer() {
            super(TriggerStatus.class);
        }

        public TriggerStatus deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            return TriggerStatus.of(p.getValueAsString());
        }
    }

    enum Enum implements TriggerStatus {
        START, STOP;

        public static Enum parse(String rawValue) {
            if (rawValue == null) {
                throw new IllegalArgumentException("Raw value cannot be null");
            }
            for (Enum enumValue : values()) {
                if (enumValue.name().equals(rawValue.toUpperCase(Locale.ROOT).trim())) {
                    return enumValue;
                }
            }
            throw new IllegalArgumentException("Cannot parse enum from raw value: " + rawValue);
        }

        @Override
        @JsonValue
        public String value() {
            return name();
        }
    }

    @Value
    class Simple implements TriggerStatus {
        String value;

        @Override
        @JsonValue
        public String value() {
            return value;
        }
    }
}
