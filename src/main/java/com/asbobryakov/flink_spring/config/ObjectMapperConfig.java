package com.asbobryakov.flink_spring.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ObjectMapperConfig {

    public static ObjectMapper createObjectMapper() {
        return new ObjectMapper()
                   .registerModule(new JavaTimeModule())
                   .registerModule(new Jdk8Module());
    }
}
