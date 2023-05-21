package com.asbobryakov.flink_spring.properties;

import org.apache.flink.api.common.time.Time;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import javax.validation.constraints.NotNull;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@ConfigurationProperties("jobs.click-to-product-job")
@ConstructorBinding
@RequiredArgsConstructor
@Getter
public class ClickToProductJobProperties {
    private final boolean enabled;
    @NotNull
    private final OperatorsProperties operators;

    @ConfigurationProperties("operators")
    @ConstructorBinding
    @RequiredArgsConstructor
    @Getter
    public static class OperatorsProperties {
        @NotNull
        private final DeduplicatorProperties deduplicator;

        @ConfigurationProperties("deduplicator")
        @ConstructorBinding
        @RequiredArgsConstructor
        @Getter
        public static class DeduplicatorProperties {
            @NotNull
            private final Map<DeduplicatorName, Duration> ttl;

            public Time getTtl(DeduplicatorName name) {
                return Optional.ofNullable(ttl.get(name))
                           .map(duration -> Time.milliseconds(duration.toMillis()))
                           .orElseThrow(() -> new IllegalArgumentException("Not found TTL by Deduplicator name: " + name));
            }

            public enum DeduplicatorName {
                APP
            }
        }
    }
}
