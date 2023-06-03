package com.asbobryakov.flink_spring.operator.filter;

import com.asbobryakov.flink_spring.schema.kafka.TriggerMessage;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.Optional;

import static com.asbobryakov.flink_spring.schema.kafka.TriggerStatus.Enum.START;
import static com.asbobryakov.flink_spring.schema.kafka.TriggerStatus.Enum.STOP;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class TriggerMessageByStatusAndUserFilter implements FilterFunction<TriggerMessage> {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean filter(TriggerMessage message) {
        return Optional.ofNullable(message.getStatus())
                .map(status -> (START.equals(status) || STOP.equals(status))
                        && message.getUserId() != null
                        && isNotBlank(message.getTriggerName()))
                .orElse(false);
    }
}
