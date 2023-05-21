package com.asbobryakov.flink_spring.operator.mapper;

import com.asbobryakov.flink_spring.schema.ClickMessage;
import com.asbobryakov.flink_spring.schema.ProductMessage;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ClickMessageToProductSinkMessageMapFunction implements FlatMapFunction<ClickMessage, ProductMessage> {
    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(ClickMessage clickMessage, Collector<ProductMessage> out) {
        try {
            final var productMessage = ProductMessage.builder()
                                           .userId(clickMessage.getUserId())
                                           .productName(clickMessage.getProductName())
                                           .object(clickMessage.getObject())
                                           .platform(clickMessage.getPlatform())
                                           .timestamp(clickMessage.getTimestamp())
                                           .build();
            out.collect(productMessage);
        } catch (Exception e) {
            log.error("Error converting ClickMessage to ProductMessage", e);
        }
    }
}
