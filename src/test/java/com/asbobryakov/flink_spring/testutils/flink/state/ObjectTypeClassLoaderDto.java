package com.asbobryakov.flink_spring.testutils.flink.state;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.net.URLClassLoader;

@Getter
@RequiredArgsConstructor
public final class ObjectTypeClassLoaderDto<T> {
    private final T object;
    private final Class<T> type;
    private final URLClassLoader classLoader;
}
