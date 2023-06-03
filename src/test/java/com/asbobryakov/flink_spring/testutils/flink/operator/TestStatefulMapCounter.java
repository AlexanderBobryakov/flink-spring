package com.asbobryakov.flink_spring.testutils.flink.operator;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("PMD.TestClassWithoutTestCases")
public class TestStatefulMapCounter<T> extends RichFlatMapFunction<T, T> {
    private static final long serialVersionUID = 1L;
    private static final AtomicInteger statefulCounter = new AtomicInteger(0);
    private TypeSerializer<T> stateSerializer;
    private Class<T> classType;

    private ValueState<T> state;

    public TestStatefulMapCounter(TypeSerializer<T> stateSerializer) {
        this.stateSerializer = stateSerializer;
    }

    public TestStatefulMapCounter(Class<T> classType) {
        this.classType = classType;
    }

    @Override
    public void open(Configuration parameters) {
        final var descriptor = stateSerializer != null
                ? new ValueStateDescriptor<>("state", stateSerializer)
                : new ValueStateDescriptor<>("state", classType);
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(T value, Collector<T> out) throws Exception {
        final var stateElement = state.value();
        if (stateElement != null) {
            statefulCounter.incrementAndGet();
        } else {
            state.update(value);
        }
        out.collect(value);
    }

    @Override
    public void close() {
        statefulCounter.set(0);
    }

    public int getStatefulCounter() {
        return statefulCounter.get();
    }
}
