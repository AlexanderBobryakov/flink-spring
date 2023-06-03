package com.asbobryakov.flink_spring.testutils.flink.minicluster;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.net.URL;
import java.util.Collection;

import static java.util.Collections.emptyList;

@UtilityClass
public class MiniClusterUtils {

    @NotNull
    public static MiniClusterWithClientResource createMiniCluster() {
        final var configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 2);
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setConfiguration(configuration)
                        .setNumberTaskManagers(2)
                        .setNumberSlotsPerTaskManager(1)
                        .build());
    }

    @SneakyThrows
    public static void startMiniClusterWithClasspath(MiniClusterWithClientResource cluster, Collection<URL> classpath) {
        cluster.before();
        TestStreamEnvironment.setAsContext(cluster.getMiniCluster(), 1, emptyList(), classpath);
    }

    @NotNull
    public static StreamExecutionEnvironment createEnvironmentWithRockDb(File rocksDbStoragePath) {
        final var env = StreamExecutionEnvironment.getExecutionEnvironment();
        final var rocksDb = new EmbeddedRocksDBStateBackend(false);
        rocksDb.setDbStoragePath(rocksDbStoragePath.toString());
        env.setStateBackend(rocksDb);
        env.getConfig().disableForceKryo();
        env.setMaxParallelism(2);
        env.setParallelism(2);
        return env;
    }
}
