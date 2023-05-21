package com.asbobryakov.flink_spring.sink;

import org.apache.flink.api.connector.sink2.Sink;

public interface SinkProvider<T> {
    Sink<T> createSink();
}

