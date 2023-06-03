package com.asbobryakov.flink_spring.operator.mapper;

import lombok.SneakyThrows;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class DeduplicatorUnitTest_byTestHarness {
    private final Time TTL = Time.milliseconds(10);
    private KeyedOneInputStreamOperatorTestHarness<String, String, String> testHarness;

    @BeforeEach
    @SneakyThrows
    void init() {
        final var deduplicator = new Deduplicator<String>(TTL);
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
            new StreamFlatMap<>(deduplicator),
            value -> value,
            Types.STRING
        );
        testHarness.open();
    }

    @AfterEach
    @SneakyThrows
    void clear() {
        testHarness.close();
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("provideUniqueEvents")
    void shouldDeduplicateMessagesByTtl(List<StreamRecord<String>> events) {
        testHarness.processElements(events);
        testHarness.setStateTtlProcessingTime(TTL.toMilliseconds() - 1);
        testHarness.processElements(events);

        final var outputEvents = testHarness.getOutput();
        assertEquals(events.size(), outputEvents.size(),
            format("Unexpected number of events after deduplication. Output events: %s", outputEvents));
        assertEquals(events, new ArrayList<>(outputEvents), "Unexpected events after deduplication");
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("provideUniqueEvents")
    void shouldNotDeduplicateBecauseOfTtlExpired(List<StreamRecord<String>> events) {
        testHarness.processElements(events);
        testHarness.setStateTtlProcessingTime(TTL.toMilliseconds() + 1);
        testHarness.processElements(events);

        final var outputEvents = testHarness.getOutput();
        assertEquals(events.size() * 2, outputEvents.size(),
            format("Unexpected number of events after deduplication. Output events: %s", outputEvents));
        final var expectedEvents = new ArrayList<>();
        expectedEvents.addAll(events);
        expectedEvents.addAll(events);
        assertEquals(expectedEvents, new ArrayList<>(outputEvents), "Unexpected events after expired ttl deduplication");
    }

    private static Stream<Arguments> provideUniqueEvents() {
        return Stream.of(
            arguments(List.of(new StreamRecord<>("key_1"), new StreamRecord<>("key_2")))
        );
    }
}