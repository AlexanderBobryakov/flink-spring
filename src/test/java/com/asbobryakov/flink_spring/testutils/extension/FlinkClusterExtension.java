package com.asbobryakov.flink_spring.testutils.extension;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

@Slf4j
@SuppressWarnings({"PMD.AvoidUsingVolatile"})
public class FlinkClusterExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {
    private static final MiniClusterWithClientResource FLINK_CLUSTER;
    private static final Lock LOCK = new ReentrantLock();
    private static volatile boolean started;

    static {
        final var configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 2);
        FLINK_CLUSTER = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setConfiguration(configuration)
                .setNumberSlotsPerTaskManager(2)
                .setNumberTaskManagers(1)
                .build());
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        LOCK.lock();
        try {
            if (!started) {
                log.info("Start Flink MiniCluster");
                started = true;
                FLINK_CLUSTER.before();
                context.getRoot().getStore(GLOBAL).put("Flink Cluster", this);
            }
        } finally {
            LOCK.unlock();
        }
    }

    @Override
    public void close() {
        log.info("Close Flink MiniCluster");
        FLINK_CLUSTER.after();
        started = false;
    }
}
