package com.asbobryakov.flink_spring.testutils.flink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.streaming.runtime.util.TestListWrapper;

import java.util.List;

@SuppressWarnings("PMD.TestClassWithoutTestCases")
public final class TestUnboundedSource<T> implements SourceFunction<T> {
    private static final long serialVersionUID = 1L;
    private final int elementsListId;

    private volatile boolean running = true;

    @SuppressWarnings("unchecked")
    public static <E> TestUnboundedSource<E> fromElements(List<E> elements) {
        final var instanceListId = TestListWrapper.getInstance().createList();
        final var list = (List<E>) TestListWrapper.getInstance().getList(instanceListId);
        list.addAll(elements);
        return new TestUnboundedSource<>(instanceListId);
    }

    private TestUnboundedSource(int elementsListId) {
        this.elementsListId = elementsListId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run(SourceContext<T> ctx) throws Exception {
        final var elements = (List<T>) TestListWrapper.getInstance().getList(elementsListId);
        for (T element : elements) {
            ctx.collect(element);
        }
        while (running) {
            // CONTINUOUS_UNBOUNDED
            Thread.sleep(500L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
