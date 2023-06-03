package com.asbobryakov.flink_spring.properties;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;

import javax.validation.constraints.NotNull;
import java.time.Duration;

@ConfigurationProperties("jobs.trigger-to-alert-job")
@ConstructorBinding
@RequiredArgsConstructor
@Getter
public class TriggerToAlertJobProperties {
    private final boolean enabled;
    @NotNull
    private final Duration stateWaiting;
}
