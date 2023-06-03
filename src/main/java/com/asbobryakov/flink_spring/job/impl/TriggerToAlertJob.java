package com.asbobryakov.flink_spring.job.impl;

import com.asbobryakov.flink_spring.job.FlinkJob;
import com.asbobryakov.flink_spring.operator.filter.TriggerMessageByStatusAndUserFilter;
import com.asbobryakov.flink_spring.operator.process.TriggerAlertProcessor;
import com.asbobryakov.flink_spring.properties.TriggerToAlertJobProperties;
import com.asbobryakov.flink_spring.schema.kafka.AlertMessage;
import com.asbobryakov.flink_spring.schema.kafka.TriggerMessage;
import com.asbobryakov.flink_spring.sink.SinkProvider;
import com.asbobryakov.flink_spring.source.SourceBinder;
import lombok.AllArgsConstructor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
@ConditionalOnProperty("jobs.trigger-to-alert-job.enabled")
public class TriggerToAlertJob extends FlinkJob {
    private final TriggerToAlertJobProperties properties;
    private final SourceBinder<TriggerMessage> sourceBinder;
    private final SinkProvider<AlertMessage> sinkProvider;

    @Override
    public void registerJob(StreamExecutionEnvironment env) {
        sourceBinder.bindSource(env)
                .filter(new TriggerMessageByStatusAndUserFilter())
                .uid("filter_trigger_message_by_status_id").name("filter_trigger_message_by_status")
                .keyBy(new TriggerAlertProcessor.TriggerAlertProcessorKeySelector())
                .process(new TriggerAlertProcessor(properties.getStateWaiting()))
                .uid("trigger_alert_processor_id").name("trigger_alert_processor")
                .sinkTo(sinkProvider.createSink()).uid("sink_alert_message_id").name("sink_alert_message");
    }
}
