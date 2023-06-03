package com.asbobryakov.flink_spring.testutils.extension;


import com.asbobryakov.flink_spring.testutils.flink.minicluster.MiniClusterUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

@Slf4j
@SuppressWarnings({"PMD.AvoidUsingVolatile"})
public class FlinkClusterExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {
    private static final MiniClusterWithClientResource FLINK_CLUSTER = MiniClusterUtils.createMiniCluster();
    private static final Lock LOCK = new ReentrantLock();
    private static volatile boolean started;

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
