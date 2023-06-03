package com.asbobryakov.flink_spring.operator.transformer;

public interface MessageTransformer<I, O> {
    O transform(I message);
}
