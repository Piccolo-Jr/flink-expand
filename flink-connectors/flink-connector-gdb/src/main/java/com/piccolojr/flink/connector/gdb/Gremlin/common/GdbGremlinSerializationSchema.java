package com.piccolojr.flink.connector.gdb.Gremlin.common;

import org.apache.flink.api.common.serialization.SerializationSchema;

public class GdbGremlinSerializationSchema<IN> implements SerializationSchema<IN>{
    @Override
    public byte[] serialize(Object o) {
        return new byte[0];
    }
}
