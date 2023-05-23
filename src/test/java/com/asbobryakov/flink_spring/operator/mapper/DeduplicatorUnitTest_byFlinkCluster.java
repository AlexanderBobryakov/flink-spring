package com.asbobryakov.flink_spring.operator.mapper;

import com.asbobryakov.flink_spring.testutils.annotation.WithFlinkCluster;
import com.asbobryakov.flink_spring.testutils.flink.sink.TestListSink;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import lombok.SneakyThrows;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@WithFlinkCluster
public class DeduplicatorUnitTest_byFlinkCluster {
    private final Time TTL = Time.milliseconds(100);

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("provideUniqueEvents")
    void shouldDeduplicateMessagesByTtl(List<String> events) {
        final var sourceEvents = new ArrayList<String>();
        sourceEvents.addAll(events);
        sourceEvents.addAll(events);
        final var env = StreamExecutionEnvironment.getExecutionEnvironment();
        final var sink = new TestListSink<String>();
        env.fromCollection(sourceEvents)
            .keyBy(value -> value)
            .flatMap(new Deduplicator<>(TTL))
            .sinkTo(sink);

        env.execute();

        final var outputEvents = sink.getHandledEvents();
        assertEquals(events.size(), outputEvents.size(),
            format("Unexpected number of events after deduplication. Output events: %s", outputEvents));
        assertEquals(events, new ArrayList<>(outputEvents), "Unexpected events after deduplication");
    }

    private static Stream<Arguments> provideUniqueEvents() {
        return Stream.of(arguments(List.of("key_1", "key_2")));
    }
}
