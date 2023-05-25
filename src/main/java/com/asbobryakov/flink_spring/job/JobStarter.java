package com.asbobryakov.flink_spring.job;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Service;

import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class JobStarter {
    private final StreamExecutionEnvironment environment;
    private final List<FlinkJob> jobs;

    @SneakyThrows
    public AutoCloseableJobClient startJobs() {
        if (jobs.isEmpty()) {
            log.info("No Jobs found for start");
            return null;
        }
        for (FlinkJob job : jobs) {
            log.info("Register job '{}'", job.getClass().getSimpleName());
            job.registerJob(environment);
        }
        return new AutoCloseableJobClient(environment.executeAsync());
    }
}