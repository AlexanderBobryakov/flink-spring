package com.asbobryakov.flink_spring.job.impl;

import com.asbobryakov.flink_spring.job.AutoCloseableJobClient;
import com.asbobryakov.flink_spring.properties.TriggerToAlertJobProperties;
import com.asbobryakov.flink_spring.schema.kafka.AlertMessage;
import com.asbobryakov.flink_spring.schema.kafka.TriggerMessage;
import com.asbobryakov.flink_spring.testutils.annotation.FlinkJobTest;
import com.asbobryakov.flink_spring.testutils.flink.sink.TestListSink;
import com.asbobryakov.flink_spring.testutils.flink.source.TestUnboundedSource;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.UUID;

import static com.asbobryakov.flink_spring.schema.kafka.TriggerStatus.Enum.START;
import static com.asbobryakov.flink_spring.schema.kafka.TriggerStatus.Enum.STOP;
import static com.asbobryakov.flink_spring.testutils.dto.TriggerMessageTestBuilder.aTriggerMessage;
import static java.lang.String.format;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

@FlinkJobTest
@SuppressWarnings("PMD.DataflowAnomalyAnalysis")
class TriggerToAlertJobUnitTest {
    @Autowired
    private StreamExecutionEnvironment environment;

    @Nested
    @TestPropertySource(properties = "jobs.trigger-to-alert-job.state-waiting=1ms")
    public class TriggerToAlertJobUnitTest_minWaitingTime {
        @Autowired
        private TriggerToAlertJobProperties properties;

        @Test
        @SneakyThrows
        void shouldCreateAlertMessageByStartTriggerMessage() {
            final var triggerMessage = aTriggerMessage().withStatus(START).build();
            final var sink = new TestListSink<AlertMessage>();
            final var source = TestUnboundedSource.fromElements(List.of(triggerMessage));
            final var job = new TriggerToAlertJob(
                    properties,
                    env -> env.addSource(source, TypeInformation.of(TriggerMessage.class)).uid("source"),
                    () -> sink
            );

            job.registerJob(environment);
            @Cleanup final var jobClient = new AutoCloseableJobClient(environment.executeAsync());

            await().atMost(ofSeconds(2))
                    .until(() -> !sink.getHandledEvents().isEmpty());
            final var out = sink.getHandledEvents();
            assertEquals(1, out.size(), format("Unexpected message count in sink: %s", out));
            final var alertMessage = out.get(0);
            assertEquals(triggerMessage.getTriggerName(), alertMessage.getTriggerName(), "Unexpected trigger name");
            assertEquals(triggerMessage.getUserId(), alertMessage.getUserId(), "Unexpected user id");
            assertEquals(triggerMessage.getTimestamp(), alertMessage.getTimestamp(), "Unexpected timestamp");
        }
    }

    @Nested
    @TestPropertySource(properties = "jobs.trigger-to-alert-job.state-waiting=1s")
    public class TriggerToAlertJobUnitTest_longWaitingTime {
        @Autowired
        private TriggerToAlertJobProperties properties;

        @Test
        @SneakyThrows
        void shouldNotCreateAlertMessageByStartWithStopTriggerMessage() {
            final var userId = UUID.randomUUID();
            final var startTriggerMessage = aTriggerMessage().withStatus(START).withUserId(userId).build();
            final var stopTriggerMessage = aTriggerMessage().withStatus(STOP).withUserId(userId).build();
            final var sink = new TestListSink<AlertMessage>();
            final var source = TestUnboundedSource.fromElements(List.of(startTriggerMessage, stopTriggerMessage));
            final var job = new TriggerToAlertJob(
                    properties,
                    env -> {
                        env.setParallelism(1);
                        return env.addSource(source, TypeInformation.of(TriggerMessage.class)).uid("source");
                    },
                    () -> sink
            );

            job.registerJob(environment);
            @Cleanup final var jobClient = new AutoCloseableJobClient(environment.executeAsync());

            await().during(ofSeconds(2))
                    .until(() -> {
                        final var userId1 = userId;
                        return sink.getHandledEvents().isEmpty();
                    });
        }
    }
}