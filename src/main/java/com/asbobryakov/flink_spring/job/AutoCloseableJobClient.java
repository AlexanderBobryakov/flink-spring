package com.asbobryakov.flink_spring.job;

import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class AutoCloseableJobClient implements JobClient, AutoCloseable {
    private final JobClient original;

    @Override
    public JobID getJobID() {
        return original.getJobID();
    }

    @Override
    public CompletableFuture<JobStatus> getJobStatus() {
        return original.getJobStatus();
    }

    @Override
    public CompletableFuture<Void> cancel() {
        return original.cancel();
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(boolean advanceToEndOfEventTime, @Nullable String savepointDirectory, SavepointFormatType formatType) {
        return original.stopWithSavepoint(advanceToEndOfEventTime, savepointDirectory, formatType);
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(@Nullable String savepointDirectory, SavepointFormatType formatType) {
        return original.triggerSavepoint(savepointDirectory, formatType);
    }

    @Override
    public CompletableFuture<Map<String, Object>> getAccumulators() {
        return original.getAccumulators();
    }

    @Override
    public CompletableFuture<JobExecutionResult> getJobExecutionResult() {
        return original.getJobExecutionResult();
    }

    @Override
    public void close() throws Exception {
        original.cancel().get(5, TimeUnit.SECONDS);
    }
}
