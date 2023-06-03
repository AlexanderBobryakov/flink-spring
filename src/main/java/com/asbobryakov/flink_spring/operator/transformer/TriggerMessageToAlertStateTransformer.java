package com.asbobryakov.flink_spring.operator.transformer;

import com.asbobryakov.flink_spring.schema.kafka.TriggerMessage;
import com.asbobryakov.flink_spring.schema.state.AlertState;

public class TriggerMessageToAlertStateTransformer implements MessageTransformer<TriggerMessage, AlertState> {

    @Override
    public AlertState transform(TriggerMessage triggerMessage) {
        return AlertState.builder()
                .userId(triggerMessage.getUserId())
                .triggerName(triggerMessage.getTriggerName())
                .timestamp(triggerMessage.getTimestamp())
                .build();
    }
}
