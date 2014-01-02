package com.mylife.hbase.mapper;

import java.io.IOException;

import com.mylife.hbase.mapper.serialization.HBaseObjectSerializer;
import com.mylife.hbase.mapper.serialization.json.JsonSerializer;
import com.mylife.hbase.mapper.serialization.kryo.KryoSerializer;

public enum SerializationStategy {
    KRYO(KryoSerializer.newInstance()), JSON(JsonSerializer.newInstance());

    private final HBaseObjectSerializer hBaseObjectSerializer;

    private SerializationStategy(HBaseObjectSerializer hBaseObjectSerializer) {
        this.hBaseObjectSerializer = hBaseObjectSerializer;

    }

    public byte[] serialize(Object object) throws IOException {
        return hBaseObjectSerializer.serialize(object);
    }

    public <T> T deserialize(byte[] byteArray, Class<T> type) throws IOException {
        return hBaseObjectSerializer.deserialize(byteArray, type);
    }

}
