package com.asbobryakov.flink_spring.testutils.flink.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.test.streaming.runtime.util.TestListWrapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("PMD.TestClassWithoutTestCases")
public class TestListSink<T> implements Sink<T> {
    private static final long serialVersionUID = 1L;

    private final ListWriter writer = new ListWriter();
    private final int resultListId;

    public TestListSink() {
        this.resultListId = TestListWrapper.getInstance().createList();
    }

    @Override
    public SinkWriter<T> createWriter(InitContext context) {
        return writer;
    }

    public List<T> getHandledEvents() {
        return new ArrayList<>(resultList());
    }

    @SuppressWarnings("unchecked")
    private List<T> resultList() {
        synchronized (TestListWrapper.getInstance()) {
            return (List<T>) TestListWrapper.getInstance().getList(resultListId);
        }
    }

    private class ListWriter implements SinkWriter<T>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public void write(T element, Context context) {
            resultList().add(element);
        }

        @Override
        public void flush(boolean endOfInput) {
            // no op
        }

        @Override
        public void close() {
            // no op
        }
    }
}
