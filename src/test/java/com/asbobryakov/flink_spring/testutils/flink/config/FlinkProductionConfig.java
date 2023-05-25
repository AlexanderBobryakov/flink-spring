package com.asbobryakov.flink_spring.testutils.flink.config;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;

@TestConfiguration
public class FlinkProductionConfig {

    @Autowired
    public void changeFlinkEnvironment(StreamExecutionEnvironment environment) {
        final var backend = new EmbeddedRocksDBStateBackend(false);
        environment.setStateBackend(backend);
    }
}
