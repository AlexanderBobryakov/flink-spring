package com.asbobryakov.flink_spring.operator.process;

import com.asbobryakov.flink_spring.operator.function.SerializablePredicate;
import lombok.RequiredArgsConstructor;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Map;

/**
 * A function that splits events of a stream by predicates into OutputTags.
 *
 * <p><b>NOTE:</b> There will be NO events in the output stream. Events will only be in OutputTags.
 *
 * <p>The basic syntax for using a StreamSpliterator is as follows:
 *
 * <pre>{@code
 * final DataStream<MyEvent> source = ...
 * final var tag_1 = new OutputTag<>("Tag_Name_1", TypeInformation.of(MyEvent.class));
 * final var tag_2 = new OutputTag<>("Tag_Name_2", TypeInformation.of(MyEvent.class));
 * final var defaultTag = new OutputTag<>("DEFAULT", TypeInformation.of(MyEvent.class));
 *
 * final var splittedSource = source.process(new StreamSpliterator<>(
 *             Map.of(
 *                 tag_1, myEvent -> <your_predicate>,
 *                 tag_2, myEvent -> <your_predicate>
 *             ),
 *             defaultTag
 *         ));
 *
 * // target splittedSource is empty
 *  final var eventsForTag1Stream = sourceByPlatform.getSideOutput(tag_1);
 *  final var eventsForTag2Stream = sourceByPlatform.getSideOutput(tag_2);
 *  final var eventsForDefaultStream= sourceByPlatform.getSideOutput(defaultTag);
 * }</pre>
 *
 * @param <I> - Type of the input elements.
 */
@RequiredArgsConstructor
public class StreamSpliterator<I> extends ProcessFunction<I, Object> {
    private static final long serialVersionUID = 1L;

    private final Map<OutputTag<I>, SerializablePredicate<I>> predicatesByTags;
    private final OutputTag<I> defaultTag;

    @Override
    public void processElement(I value, ProcessFunction<I, Object>.Context ctx, Collector<Object> out) {
        for (Map.Entry<OutputTag<I>, SerializablePredicate<I>> entry : predicatesByTags.entrySet()) {
            final var predicate = entry.getValue();
            if (predicate.test(value)) {
                final var tag = entry.getKey();
                ctx.output(tag, value);
                return;
            }
        }
        ctx.output(defaultTag, value);
    }
}
