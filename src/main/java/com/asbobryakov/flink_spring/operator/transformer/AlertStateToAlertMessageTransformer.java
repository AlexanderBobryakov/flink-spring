package com.asbobryakov.flink_spring.operator.transformer;

import com.asbobryakov.flink_spring.schema.kafka.AlertMessage;
import com.asbobryakov.flink_spring.schema.state.AlertState;

public class AlertStateToAlertMessageTransformer implements MessageTransformer<AlertState, AlertMessage> {

    @Override
    public AlertMessage transform(AlertState alertState) {
        return AlertMessage.builder()
                .userId(alertState.getUserId())
                .triggerName(alertState.getTriggerName())
                .timestamp(alertState.getTimestamp())
                .build();
    }
}
