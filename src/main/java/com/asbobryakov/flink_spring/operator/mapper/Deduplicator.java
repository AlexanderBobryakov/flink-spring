package com.asbobryakov.flink_spring.operator.mapper;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.StateTtlConfig.UpdateType;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.flink.api.common.state.StateTtlConfig.StateVisibility.NeverReturnExpired;

public class Deduplicator<T> extends RichFlatMapFunction<T, T> {
    private static final long serialVersionUID = 1L;

    private final Time ttl;
    private ValueState<Boolean> keyHasBeenSeen;

    public Deduplicator(Time ttl) {
        this.ttl = notNull(ttl, "Deduplicator TTL is null");
    }

    @Override
    public void open(Configuration parameters) {
        final var descriptor = new ValueStateDescriptor<>("keyHasBeenSeen", Types.BOOLEAN);
        final var ttlConfig = StateTtlConfig
                                  .newBuilder(ttl)
                                  .setUpdateType(UpdateType.OnCreateAndWrite)
                                  .setStateVisibility(NeverReturnExpired)
                                  .cleanupFullSnapshot()
                                  .build();
        descriptor.enableTimeToLive(ttlConfig);
        keyHasBeenSeen = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(T event, Collector<T> out) throws Exception {
        if (keyHasBeenSeen.value() == null) {
            out.collect(event);
            keyHasBeenSeen.update(true);
        }
    }
}
