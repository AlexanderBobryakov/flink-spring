package com.asbobryakov.flink_spring.schema.annotation;

import com.asbobryakov.flink_spring.serialization.JacksonStateSerializer;
import com.asbobryakov.flink_spring.testutils.flink.operator.TestStatefulMapCounter;
import com.asbobryakov.flink_spring.testutils.flink.sink.TestListSink;
import com.asbobryakov.flink_spring.testutils.flink.source.TestUnboundedSource;
import com.asbobryakov.flink_spring.testutils.flink.state.StateClassLoadingUtil;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.reflections.Reflections;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
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

@Slf4j
@SuppressWarnings({"PMD.UseVarargs", "PMD.UseProperClassLoader", "PMD.AddEmptyString"})
public class JacksonEvolvingStateUnitTest {
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
    @MethodSource("provideClassWithEvolvedVersionsPairs")
    @SneakyThrows
    void shouldSuccessRestoredBetweenEvolvedStates(String className, int currentVersion, int evolvedVersion) {
        // job to store base state in savepoint
        final var objectTypeClassLoaderDto = compileStateClass(className, currentVersion);

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

    private static Stream<Arguments> provideClassWithEvolvedVersionsPairs() {
        Stream<Arguments> result = Stream.empty();
        final var reflections = new Reflections("com.asbobryakov.flink_spring");
        final var evolvingStates = reflections.getTypesAnnotatedWith(JacksonEvolvingState.class);
        for (Class<?> evolvingStateClass : evolvingStates) {
            final var className = evolvingStateClass.getSimpleName();
            final var annotation = evolvingStateClass.getAnnotation(JacksonEvolvingState.class);
            final var annotationVersion = annotation.version();
            assertLastVersionsInSrcAndResourcesAreEquals(evolvingStateClass, annotationVersion);
            for (int i = 1; i < annotationVersion; i++) {
                log.info("Evolving State '{}' v{} -> v{} will be tested", className, i, i + 1);
                result = Stream.concat(result, Stream.of(arguments(className, i, i + 1)));
            }
            log.info("Evolving State '{}' v{} -> v{} will be tested", className, annotationVersion, annotationVersion);
            result = Stream.concat(result, Stream.of(arguments(className, annotationVersion, annotationVersion)));
        }
        return result;
    }

    @SneakyThrows
    private static void assertLastVersionsInSrcAndResourcesAreEquals(Class<?> clazz, int lastVersion) {
        final var srcClassContent = IOUtils.toString(
                Paths.get("src", "main", "java", clazz.getName().replaceAll("\\.", "/") + ".java").toUri(),
                StandardCharsets.UTF_8);
        final var srcClassContentWithoutPackage = srcClassContent.substring(srcClassContent.indexOf("\n")).replaceAll("\n", "");
        final var classSimpleName = clazz.getSimpleName();
        final var resourceClassContent = StateClassLoadingUtil.loadStateClassContentFromResources(classSimpleName, lastVersion).replaceAll("\n", "");
        assertEquals(srcClassContentWithoutPackage, resourceClassContent,
                format("The content of class '%s' version '%s' in /src differs from the last one set in resources/state/%s. " +
                                "The schema version in /src needs to be promoted. See the scheme evolution block in Readme for details.",
                        classSimpleName, lastVersion, classSimpleName));
    }
}