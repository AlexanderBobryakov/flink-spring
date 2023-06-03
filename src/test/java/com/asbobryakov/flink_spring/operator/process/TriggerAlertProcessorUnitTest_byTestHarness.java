package com.asbobryakov.flink_spring.operator.process;

import com.asbobryakov.flink_spring.schema.kafka.AlertMessage;
import com.asbobryakov.flink_spring.schema.kafka.TriggerMessage;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static com.asbobryakov.flink_spring.schema.kafka.TriggerStatus.Enum.START;
import static com.asbobryakov.flink_spring.schema.kafka.TriggerStatus.Enum.STOP;
import static com.asbobryakov.flink_spring.testutils.dto.TriggerMessageTestBuilder.aTriggerMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class TriggerAlertProcessorUnitTest_byTestHarness {
    private final Duration stateWaiting = Duration.ofMillis(10);
    private KeyedOneInputStreamOperatorTestHarness<String, TriggerMessage, AlertMessage> testHarness;

    @BeforeEach
    @SneakyThrows
    void init() {
        final var processor = new TriggerAlertProcessor(stateWaiting);
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(processor),
                new TriggerAlertProcessor.TriggerAlertProcessorKeySelector(),
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
    @MethodSource("provideStartEvents")
    void shouldProcessStartEventsWhenTimerInvoked(List<StreamRecord<TriggerMessage>> events) {
        testHarness.processElements(events);
        testHarness.setProcessingTime(stateWaiting.toMillis() + 1);

        final var outputEvents = testHarness.extractOutputStreamRecords();
        assertEquals(events.size(), outputEvents.size(), "Unexpected size of output result list");
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("provideStartEvents")
    void shouldNotProcessStartEventsWhenTimerNotInvoked(List<StreamRecord<TriggerMessage>> events) {
        testHarness.processElements(events);

        final var outputEvents = testHarness.extractOutputStreamRecords();
        assertEquals(0, outputEvents.size(), "Unexpected size of output result list");
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("provideStartStopEvents")
    void shouldNotProcessEventsWhenStartWithStop(List<StreamRecord<TriggerMessage>> events) {
        testHarness.processElements(events);
        testHarness.setProcessingTime(stateWaiting.toMillis() + 1);

        final var outputEvents = testHarness.extractOutputStreamRecords();
        assertEquals(0, outputEvents.size(), "Unexpected size of output result list");
    }

    private static Stream<Arguments> provideStartEvents() {
        return Stream.of(
                arguments(List.of(
                        new StreamRecord<>(aTriggerMessage().withUserId(UUID.randomUUID()).withStatus(START).withTriggerName("trigger_1").build()),
                        new StreamRecord<>(aTriggerMessage().withUserId(UUID.randomUUID()).withStatus(START).withTriggerName("trigger_1").build()),
                        new StreamRecord<>(aTriggerMessage().withUserId(UUID.randomUUID()).withStatus(START).withTriggerName("trigger_2").build())
                ))
        );
    }

    private static Stream<Arguments> provideStartStopEvents() {
        final var userId = UUID.randomUUID();
        return Stream.of(
                arguments(List.of(
                        new StreamRecord<>(aTriggerMessage().withUserId(userId).withStatus(START).withTriggerName("trigger_3").build()),
                        new StreamRecord<>(aTriggerMessage().withUserId(userId).withStatus(STOP).withTriggerName("trigger_3").build())
                ))
        );
    }
}