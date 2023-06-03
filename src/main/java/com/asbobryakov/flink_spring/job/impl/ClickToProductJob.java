package com.asbobryakov.flink_spring.job.impl;

import com.asbobryakov.flink_spring.job.FlinkJob;
import com.asbobryakov.flink_spring.operator.filter.ClickMessageWithPlatformFilter;
import com.asbobryakov.flink_spring.operator.mapper.ClickMessageToWrappedProductSinkMessageMapFunction;
import com.asbobryakov.flink_spring.operator.mapper.Deduplicator;
import com.asbobryakov.flink_spring.operator.process.StreamSpliterator;
import com.asbobryakov.flink_spring.properties.ClickToProductJobProperties;
import com.asbobryakov.flink_spring.properties.ClickToProductJobProperties.OperatorsProperties.DeduplicatorProperties.DeduplicatorName;
import com.asbobryakov.flink_spring.schema.kafka.ClickMessage;
import com.asbobryakov.flink_spring.schema.kafka.Platform;
import com.asbobryakov.flink_spring.schema.kafka.ProductMessage;
import com.asbobryakov.flink_spring.schema.kafka.WrappedSinkMessage;
import com.asbobryakov.flink_spring.sink.SinkProvider;
import com.asbobryakov.flink_spring.source.SourceBinder;
import lombok.AllArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@AllArgsConstructor
@ConditionalOnProperty("jobs.click-to-product-job.enabled")
public class ClickToProductJob extends FlinkJob {
    private final ClickToProductJobProperties properties;
    private final SourceBinder<ClickMessage> sourceBinder;
    private final SinkProvider<WrappedSinkMessage<ProductMessage>> sinkProvider;

    @Override
    public void registerJob(StreamExecutionEnvironment env) {
        final var webTag = new OutputTag<>("WEB", TypeInformation.of(ClickMessage.class));
        final var appTag = new OutputTag<>("APP", TypeInformation.of(ClickMessage.class));

        final var splittedByAppAndWebStream = sourceBinder.bindSource(env)
                                                  .filter(new ClickMessageWithPlatformFilter())
                                                  .uid("filter_click_message_by_platform_id").name("filter_click_message_by_platform")
                                                  .process(new StreamSpliterator<>(
                                                      Map.of(
                                                          webTag, clickMessage -> Platform.Enum.WEB.equals(clickMessage.getPlatform()),
                                                          appTag, clickMessage -> Platform.Enum.APP.equals(clickMessage.getPlatform())),
                                                      new OutputTag<>("UNKNOWN", TypeInformation.of(ClickMessage.class))
                                                  )).uid("split_click_message_by_platform_id").name("split_click_message_by_platform");
        final var appStream = splittedByAppAndWebStream.getSideOutput(appTag);
        final var webStream = splittedByAppAndWebStream.getSideOutput(webTag);

        final var deduplicatedAppStream = appStream.keyBy(clickMessage -> clickMessage.getUserId().toString() + clickMessage.getTimestamp())
                                              .flatMap(new Deduplicator<>(properties.getOperators().getDeduplicator().getTtl(DeduplicatorName.APP)))
                                              .uid("deduplicate_app_messages_id").name("deduplicate_app_messages");

        final var sink = sinkProvider.createSink();
        webStream.union(deduplicatedAppStream)
            .flatMap(new ClickMessageToWrappedProductSinkMessageMapFunction())
            .uid("map_click_to_product_id").name("map_click_to_product")
            .sinkTo(sink)
            .uid("sink_product_message_id").name("sink_product_message");
    }
}
