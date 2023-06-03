package com.asbobryakov.flink_spring.operator.process;

import com.asbobryakov.flink_spring.job.AutoCloseableJobClient;
import com.asbobryakov.flink_spring.schema.kafka.AlertMessage;
import com.asbobryakov.flink_spring.schema.kafka.TriggerMessage;
import com.asbobryakov.flink_spring.testutils.annotation.WithFlinkCluster;
import com.asbobryakov.flink_spring.testutils.flink.sink.TestListSink;
import com.asbobryakov.flink_spring.testutils.flink.source.TestUnboundedSource;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.asbobryakov.flink_spring.schema.kafka.TriggerStatus.Enum.START;
import static com.asbobryakov.flink_spring.schema.kafka.TriggerStatus.Enum.STOP;
import static com.asbobryakov.flink_spring.testutils.dto.TriggerMessageTestBuilder.aTriggerMessage;
import static java.time.Duration.*;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@WithFlinkCluster
@SuppressWarnings({"PMD.AvoidDuplicateLiterals", "PMD.DataflowAnomalyAnalysis"})
class TriggerAlertProcessorUnitTest_byFlinkMiniCluster {

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("provideStartEvents")
    @Disabled("BOUNDED Source problem")
    void shouldProcessStartEventsWhenTimerInvoked_v1(List<TriggerMessage> events) {
        final var env = StreamExecutionEnvironment.getExecutionEnvironment();
        final var sink = new TestListSink<AlertMessage>();

        env.fromCollection(events)
                .keyBy(new TriggerAlertProcessor.TriggerAlertProcessorKeySelector())
                .process(new TriggerAlertProcessor(ofMillis(15)))
                .sinkTo(sink);
        env.execute();

        await().atMost(ofSeconds(60))
                .until(() -> sink.getHandledEvents().size() == events.size());
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("provideStartEvents")
    @Disabled("Not finished test")
    void shouldProcessStartEventsWhenTimerInvoked_v2(List<TriggerMessage> events) {
        final var env = StreamExecutionEnvironment.getExecutionEnvironment();
        final var sink = new TestListSink<AlertMessage>();
        final var source = TestUnboundedSource.fromElements(events);

        env.addSource(source, TypeInformation.of(TriggerMessage.class))
                .keyBy(new TriggerAlertProcessor.TriggerAlertProcessorKeySelector())
                .process(new TriggerAlertProcessor(ofMillis(15)))
                .sinkTo(sink);
        env.execute();

        await().atMost(ofSeconds(2))
                .until(() -> sink.getHandledEvents().size() == events.size());
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("provideStartEvents")
    @Disabled("Not closeable resources after assert fail")
    void shouldProcessStartEventsWhenTimerInvoked_v3(List<TriggerMessage> events) {
        final var env = StreamExecutionEnvironment.getExecutionEnvironment();
        final var sink = new TestListSink<AlertMessage>();
        final var source = TestUnboundedSource.fromElements(events);

        env.addSource(source, TypeInformation.of(TriggerMessage.class))
                .keyBy(new TriggerAlertProcessor.TriggerAlertProcessorKeySelector())
                .process(new TriggerAlertProcessor(ofMillis(15)))
                .sinkTo(sink);
        final var jobClient = env.executeAsync();

        await().atMost(ofSeconds(2))
                .until(() -> sink.getHandledEvents().size() == events.size());
        jobClient.cancel().get(2, TimeUnit.SECONDS);
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("provideStartEvents")
    void shouldProcessStartEventsWhenTimerInvoked(List<TriggerMessage> events) {
        final var env = StreamExecutionEnvironment.getExecutionEnvironment();
        final var sink = new TestListSink<AlertMessage>();
        final var source = TestUnboundedSource.fromElements(events);

        env.addSource(source, TypeInformation.of(TriggerMessage.class))
                .keyBy(new TriggerAlertProcessor.TriggerAlertProcessorKeySelector())
                .process(new TriggerAlertProcessor(ofMillis(15)))
                .sinkTo(sink);
        @Cleanup final var jobClient = new AutoCloseableJobClient(env.executeAsync());

        await().atMost(ofSeconds(2))
                .until(() -> sink.getHandledEvents().size() == events.size());
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("provideStartEvents")
    void shouldNotProcessStartEventsWhenTimerNotInvoked(List<TriggerMessage> events) {
        final var env = StreamExecutionEnvironment.getExecutionEnvironment();
        final var sink = new TestListSink<AlertMessage>();
        final var source = TestUnboundedSource.fromElements(events);

        env.addSource(source, TypeInformation.of(TriggerMessage.class))
                .keyBy(new TriggerAlertProcessor.TriggerAlertProcessorKeySelector())
                .process(new TriggerAlertProcessor(ofDays(1)))
                .sinkTo(sink);
        @Cleanup final var jobClient = new AutoCloseableJobClient(env.executeAsync());

        await().during(ofSeconds(2))
                .until(() -> sink.getHandledEvents().isEmpty());
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("provideStartStopEvents")
    void shouldNotProcessEventsWhenStartWithStop(List<TriggerMessage> events) {
        final var env = StreamExecutionEnvironment.getExecutionEnvironment();
        final var sink = new TestListSink<AlertMessage>();
        final var source = TestUnboundedSource.fromElements(events);

        env.addSource(source, TypeInformation.of(TriggerMessage.class))
                .keyBy(new TriggerAlertProcessor.TriggerAlertProcessorKeySelector())
                .process(new TriggerAlertProcessor(ofMillis(15)))
                .sinkTo(sink);
        @Cleanup final var jobClient = new AutoCloseableJobClient(env.executeAsync());

        await().during(ofSeconds(2))
                .until(() -> sink.getHandledEvents().isEmpty());
    }

    private static Stream<Arguments> provideStartEvents() {
        return Stream.of(
                arguments(List.of(
                        aTriggerMessage().withUserId(UUID.randomUUID()).withStatus(START).withTriggerName("trigger_1").build(),
                        aTriggerMessage().withUserId(UUID.randomUUID()).withStatus(START).withTriggerName("trigger_1").build(),
                        aTriggerMessage().withUserId(UUID.randomUUID()).withStatus(START).withTriggerName("trigger_2").build()
                ))
        );
    }

    private static Stream<Arguments> provideStartStopEvents() {
        final var userId = UUID.randomUUID();
        return Stream.of(
                arguments(List.of(
                        aTriggerMessage().withUserId(userId).withStatus(START).withTriggerName("trigger_3").build(),
                        aTriggerMessage().withUserId(userId).withStatus(STOP).withTriggerName("trigger_3").build()
                ))
        );
    }
}