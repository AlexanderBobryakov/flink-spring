package com.asbobryakov.flink_spring;

import com.asbobryakov.flink_spring.job.JobStarter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty("flink.submit-jobs-on-app-start")
public class AppListener {
    private final JobStarter jobStarter;

    @EventListener(ApplicationStartedEvent.class)
    @SneakyThrows
    public void onApplicationStart() {
        jobStarter.startJobs();
    }
}
