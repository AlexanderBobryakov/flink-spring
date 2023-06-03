package com.asbobryakov.flink_spring.serialization;

import com.asbobryakov.flink_spring.testutils.flink.operator.TestStatefulMapCounter;
import com.asbobryakov.flink_spring.testutils.flink.sink.TestListSink;
import com.asbobryakov.flink_spring.testutils.flink.source.TestUnboundedSource;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.asbobryakov.flink_spring.testutils.flink.minicluster.MiniClusterUtils.*;
import static com.asbobryakov.flink_spring.testutils.flink.state.StateClassLoadingUtil.compileStateClass;
import static java.lang.String.format;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.core.execution.SavepointFormatType.NATIVE;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@SuppressWarnings({"PMD.UseVarargs", "PMD.UseProperClassLoader", "PMD.AddEmptyString"})
class JacksonStateSerializerUnitTest {
    @TempDir
    private Path tmpFolder;
    private File savepointDir;
    private File rocksDbDir;

    @BeforeEach
    @SneakyThrows
    void init() {
        savepointDir = new File(tmpFolder.toFile(), "savepoints");
        rocksDbDir = new File(tmpFolder.toFile(), "rocksDb");
        if (!savepointDir.mkdirs() || !rocksDbDir.mkdirs()) {
            fail(format("Test setup failed: failed to create temporary directories: %s",
                    List.of(savepointDir.toString(), rocksDbDir.toString())));
        }
    }

    @ParameterizedTest
    @MethodSource("evolveClasses")
    @SneakyThrows
    @Timeout(value = 15, unit = SECONDS)
    void shouldRestoreEvolvedState(String className, int baseVersion, int evolvedVersion) {
        // job to store base state in savepoint
        final var objectTypeClassLoaderDto = compileStateClass(className, baseVersion);

        var cluster = createMiniCluster();
        startMiniClusterWithClasspath(cluster, Arrays.asList(objectTypeClassLoaderDto.getClassLoader().getURLs()));
        final var jacksonStateSerializer = new JacksonStateSerializer<>(objectTypeClassLoaderDto.getType());
        final var statefulCounter = new TestStatefulMapCounter<>(jacksonStateSerializer);
        final var fullObjectSink = new TestListSink<>();
        @Cleanup final var env = createEnvironmentWithRockDb(rocksDbDir);

        final var object = objectTypeClassLoaderDto.getObject();
        env.addSource(TestUnboundedSource.fromElements(List.of(object)))
                .returns(objectTypeClassLoaderDto.getType())
                .keyBy(value -> "static_key")
                .flatMap(statefulCounter).uid("id_stateful")
                .sinkTo(fullObjectSink);
        final var jobClient = env.executeAsync(env.getStreamGraph());
        await()
                .atMost(ofSeconds(5))
                .until(() -> fullObjectSink.getHandledEvents().size() == 1);
        assertEquals(0, statefulCounter.getStatefulCounter(), "Stateful counter has state for current object key");
        final var savepoint = jobClient.triggerSavepoint(savepointDir.toString(), NATIVE).get(2, SECONDS);
        jobClient.cancel().get(2, SECONDS);
        cluster.after();

        // job with restored state from savepoint for evolved schema
        final var evolvedObjectTypeClassLoaderDto = compileStateClass(className, evolvedVersion);

        cluster = createMiniCluster();
        startMiniClusterWithClasspath(cluster, Arrays.asList(evolvedObjectTypeClassLoaderDto.getClassLoader().getURLs()));
        final var evolvedJacksonStateSerializer = new JacksonStateSerializer<>(evolvedObjectTypeClassLoaderDto.getType());
        final var evolvedStatefulCounter = new TestStatefulMapCounter<>(evolvedJacksonStateSerializer);
        final var evolvedSink = new TestListSink<>();
        @Cleanup final var evolvedEnv = createEnvironmentWithRockDb(rocksDbDir);
        final var evolvedObject = evolvedObjectTypeClassLoaderDto.getObject();
        evolvedEnv.addSource(TestUnboundedSource.fromElements(List.of(evolvedObject)))
                .returns(evolvedObjectTypeClassLoaderDto.getType())
                .keyBy(value -> "static_key")
                .flatMap(evolvedStatefulCounter).uid("id_stateful")
                .sinkTo(evolvedSink);
        final var streamGraph = evolvedEnv.getStreamGraph();
        streamGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepoint));
        final var evolvedJobClient = evolvedEnv.executeAsync(streamGraph);
        await()
                .atMost(ofSeconds(5))
                .until(() -> evolvedSink.getHandledEvents().size() == 1);
        assertEquals(1, evolvedStatefulCounter.getStatefulCounter(),
                "Stateful counter has no state for the current object key, which must be restored from a savepoint");
        evolvedJobClient.cancel().get(2, SECONDS);
        cluster.after();
    }

    private static Stream<Arguments> evolveClasses() {
        final var className = "TestEvolvedClass";
        return Stream.of(
                arguments(className, 1, 2),
                arguments(className, 2, 1)
        );
    }
}