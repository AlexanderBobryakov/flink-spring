package com.asbobryakov.flink_spring.schema;

import java.io.Serializable;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@EqualsAndHashCode
@RequiredArgsConstructor
public class WrappedSinkMessage<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Meta meta;
    private final T message;

    @Getter
    @EqualsAndHashCode
    @RequiredArgsConstructor
    public static class Meta implements Serializable {
        private static final long serialVersionUID = 1L;

        public final String targetTopicName;
    }
}
