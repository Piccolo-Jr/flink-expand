package com.piccolojr.flink.connector.gdb.Gremlin.common;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public interface GdbGremlinDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    default void open(DeserializationSchema.InitializationContext context) throws Exception {}

    default boolean isEndOfStream(T nextElement)throws Exception{return true;}

    T deserialize(Tuple message) throws Exception;

    default void deserialize(Tuple message, Collector<T> out) throws Exception {
        T deserialized = deserialize(message);
        if (deserialized != null) {
            out.collect(deserialized);
        }
    }
}
