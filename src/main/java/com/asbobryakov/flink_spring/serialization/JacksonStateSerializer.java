package com.asbobryakov.flink_spring.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeutils.GenericTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

import static com.asbobryakov.flink_spring.config.ObjectMapperConfig.createObjectMapper;

public class JacksonStateSerializer<T> extends TypeSerializer<T> {
    private static final long serialVersionUID = 1;

    private final Class<T> typeClass;
    private final transient ObjectMapper mapper;

    public JacksonStateSerializer(Class<T> typeClass) {
        this.typeClass = typeClass;
        this.mapper = createObjectMapper();
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<T> duplicate() {
        return new JacksonStateSerializer<>(typeClass);
    }

    @Override
    public T createInstance() {
        return null;
    }

    @Override
    public T copy(T from) {
        return mapper.convertValue(from, typeClass);
    }

    @Override
    public T copy(T from, T reuse) {
        return copy(from);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        final var bytes = mapper.writeValueAsBytes(record);
        target.writeInt(bytes.length);
        target.write(bytes);
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        final var byteLength = source.readInt();
        final var recordBytes = new byte[byteLength];
        source.readFully(recordBytes);
        return mapper.readValue(recordBytes, typeClass);
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == JacksonStateSerializer.class) {
            final var that = (JacksonStateSerializer<T>) obj;
            return this.typeClass == that.typeClass;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return typeClass.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        return new JacksonSerializerSnapshot<>(typeClass);
    }

    @SuppressWarnings("unused")
    public static final class JacksonSerializerSnapshot<T> extends GenericTypeSerializerSnapshot<T, JacksonStateSerializer<T>> {
        private static final long serialVersionUID = 1;

        private Class<T> typeClass;

        public JacksonSerializerSnapshot() {
            // used for flink reflection
        }

        public JacksonSerializerSnapshot(Class<T> typeClass) {
            super(typeClass);
            this.typeClass = typeClass;
        }

        @Override
        protected TypeSerializer<T> createSerializer(Class<T> typeClass) {
            return new JacksonStateSerializer<>(typeClass);
        }

        @Override
        protected Class<T> getTypeClass(JacksonStateSerializer<T> serializer) {
            return typeClass != null ? typeClass : serializer.typeClass;
        }

        @Override
        protected Class<?> serializerClass() {
            return JacksonStateSerializer.class;
        }
    }
}
