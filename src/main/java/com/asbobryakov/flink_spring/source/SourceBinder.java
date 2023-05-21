package com.asbobryakov.flink_spring.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface SourceBinder<T> {
    DataStream<T> bindSource(StreamExecutionEnvironment flink);
}
