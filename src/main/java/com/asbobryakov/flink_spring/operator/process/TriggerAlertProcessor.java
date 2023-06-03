package com.asbobryakov.flink_spring.operator.process;

import com.asbobryakov.flink_spring.operator.transformer.AlertStateToAlertMessageTransformer;
import com.asbobryakov.flink_spring.operator.transformer.MessageTransformer;
import com.asbobryakov.flink_spring.operator.transformer.TriggerMessageToAlertStateTransformer;
import com.asbobryakov.flink_spring.schema.kafka.AlertMessage;
import com.asbobryakov.flink_spring.schema.kafka.TriggerMessage;
import com.asbobryakov.flink_spring.schema.state.AlertState;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import javax.validation.constraints.NotNull;
import java.time.Duration;

import static com.asbobryakov.flink_spring.schema.kafka.TriggerStatus.Enum.START;
import static com.asbobryakov.flink_spring.schema.kafka.TriggerStatus.Enum.STOP;
import static org.apache.flink.api.common.state.StateTtlConfig.StateVisibility.NeverReturnExpired;

@Slf4j
public class TriggerAlertProcessor extends KeyedProcessFunction<String, TriggerMessage, AlertMessage> {
    private static final long serialVersionUID = 1L;

    private final Duration stateWaiting;
    private ValueState<AlertState> alertState;
    private ValueState<Long> timestampState;
    private transient MessageTransformer<TriggerMessage, AlertState> messageToStateTransformer;
    private transient MessageTransformer<AlertState, AlertMessage> stateToMessageTransformer;

    public TriggerAlertProcessor(@NotNull Duration stateWaiting) {
        this.stateWaiting = stateWaiting;
    }

    @Override
    public void open(Configuration parameters) {
        messageToStateTransformer = new TriggerMessageToAlertStateTransformer();
        stateToMessageTransformer = new AlertStateToAlertMessageTransformer();
        final var defaultTtlConfig = StateTtlConfig
                .newBuilder(Time.minutes(stateWaiting.toMillis() + 1000))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();

        // alert state
        final var alertDescriptor = new ValueStateDescriptor<>("alertState", AlertState.class);
        alertDescriptor.enableTimeToLive(defaultTtlConfig);
        alertState = getRuntimeContext().getState(alertDescriptor);

        // timestamp state
        final var timestampDescriptor = new ValueStateDescriptor<>("timestampState", Types.LONG);
        timestampDescriptor.enableTimeToLive(defaultTtlConfig);
        timestampState = getRuntimeContext().getState(timestampDescriptor);
    }

    @Override
    public void processElement(TriggerMessage message,
                               KeyedProcessFunction<String, TriggerMessage, AlertMessage>.Context ctx,
                               Collector<AlertMessage> out) throws Exception {
        final var status = message.getStatus();
        if (START.equals(status)) {
            // create timer
            final var state = messageToStateTransformer.transform(message);
            alertState.update(state);
            final var invokeTimerMillis = ctx.timerService().currentProcessingTime() + stateWaiting.toMillis();
            final var previousTimestamp = timestampState.value();
            if (previousTimestamp != null) {
                ctx.timerService().deleteProcessingTimeTimer(previousTimestamp);
            }
            ctx.timerService().registerProcessingTimeTimer(invokeTimerMillis);
            timestampState.update(invokeTimerMillis);
        } else if (STOP.equals(status)) {
            // remove timer
            final var invokeTimerMillis = timestampState.value();
            if (invokeTimerMillis != null) {
                ctx.timerService().deleteProcessingTimeTimer(invokeTimerMillis);
                timestampState.clear();
            }
            alertState.clear();
        } else {
            log.debug("Unknown trigger status {} for key {}", status, ctx.getCurrentKey());
        }
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedProcessFunction<String, TriggerMessage, AlertMessage>.OnTimerContext ctx,
                        Collector<AlertMessage> out) throws Exception {
        final var alertStateValue = alertState.value();
        if (alertStateValue != null) {
            final var alertMessage = stateToMessageTransformer.transform(alertStateValue);
            out.collect(alertMessage);
        }
        timestampState.clear();
        alertState.clear();
    }

    public static class TriggerAlertProcessorKeySelector implements KeySelector<TriggerMessage, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(TriggerMessage message) {
            return message.getUserId() + message.getTriggerName();
        }
    }
}
