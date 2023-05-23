package com.asbobryakov.flink_spring.job.impl;

import com.asbobryakov.flink_spring.properties.ClickToProductJobProperties;
import com.asbobryakov.flink_spring.schema.Platform;
import com.asbobryakov.flink_spring.schema.ProductMessage;
import com.asbobryakov.flink_spring.schema.WrappedSinkMessage;
import com.asbobryakov.flink_spring.testutils.annotation.FlinkJobTest;
import com.asbobryakov.flink_spring.testutils.flink.sink.TestListSink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;

import lombok.SneakyThrows;

import static com.asbobryakov.flink_spring.testutils.dto.ClickMessageTestBuilder.aClickMessage;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@FlinkJobTest
@SuppressWarnings("PMD.DataflowAnomalyAnalysis")
class ClickToProductJobUnitTest {
    @Autowired
    private StreamExecutionEnvironment environment;
    @Autowired
    private ClickToProductJobProperties properties;

    @ParameterizedTest
    @EnumSource(value = Platform.Enum.class, names = {"APP"})
    @SneakyThrows
    void shouldDeduplicateClickMessages(Platform platform) {
        final var message = aClickMessage().withPlatform(platform).build();
        final var sink = new TestListSink<WrappedSinkMessage<ProductMessage>>();
        final var job = new ClickToProductJob(
            properties,
            env -> env.fromElements(message, message, message).uid("test_source"),
            () -> sink
        );

        job.registerJob(environment);
        environment.execute();

        final var out = sink.getHandledEvents();
        assertEquals(1, out.size(), format("Unexpected message count in sink: %s", out));
    }

    @ParameterizedTest
    @EnumSource(value = Platform.Enum.class, names = {"WEB"})
    @SneakyThrows
    void shouldNotDeduplicateClickMessages(Platform platform) {
        final var message = aClickMessage().withPlatform(platform).build();
        final var sink = new TestListSink<WrappedSinkMessage<ProductMessage>>();
        final var job = new ClickToProductJob(
            properties,
            env -> env.fromElements(message, message, message).uid("test_source"),
            () -> sink
        );

        job.registerJob(environment);
        environment.execute();

        final var out = sink.getHandledEvents();
        assertEquals(3, out.size(), format("Unexpected message count in sink: %s", out));
    }

    @Test
    @SneakyThrows
    void shouldSkipClickMessageWithUnknownPlatform() {
        final var message = aClickMessage().withPlatform(Platform.of("unknown")).build();
        final var sink = new TestListSink<WrappedSinkMessage<ProductMessage>>();
        final var job = new ClickToProductJob(
            properties,
            env -> env.fromElements(message).uid("test_source"),
            () -> sink
        );

        job.registerJob(environment);
        environment.execute();

        final var out = sink.getHandledEvents();
        assertTrue(out.isEmpty(), format("Unexpected message count in sink: %s", out));
    }
}