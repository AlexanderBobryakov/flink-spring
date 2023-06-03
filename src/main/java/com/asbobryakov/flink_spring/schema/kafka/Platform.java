package com.asbobryakov.flink_spring.schema.kafka;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import lombok.Value;

import java.io.IOException;
import java.util.Locale;

import static java.util.Objects.requireNonNullElse;

public interface Platform {
    String value();

    static Platform of(String value) {
        final String stringValue = requireNonNullElse(value, "");
        try {
            return Platform.Enum.parse(stringValue);
        } catch (IllegalArgumentException var3) {
            return new Simple(value);
        }
    }

    class Deserializer extends StdDeserializer<Platform> {
        private static final long serialVersionUID = 1L;

        protected Deserializer() {
            super(Platform.class);
        }

        @Override
        public Platform deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            return Platform.of(p.getValueAsString());
        }
    }

    enum Enum implements Platform {
        WEB, APP, SMART_TV, CONSOLE;

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
    class Simple implements Platform {
        String value;

        @Override
        @JsonValue
        public String value() {
            return value;
        }
    }
}