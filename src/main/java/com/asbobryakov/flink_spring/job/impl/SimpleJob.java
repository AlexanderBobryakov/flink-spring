package com.asbobryakov.flink_spring.job.impl;

import com.asbobryakov.flink_spring.job.FlinkJob;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Component;

@Component
public class SimpleJob extends FlinkJob {

    @Override
    public void registerJob(StreamExecutionEnvironment env) {
        env.fromElements("value_1", "value_2", "value_3")
            .map(value -> "after_map_" + value)
            .print();
    }
}