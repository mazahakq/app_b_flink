package ru.mazahakq.appb;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class CustomByteArrayDeserializer implements DeserializationSchema<byte[]> {

    @Override
    public byte[] deserialize(byte[] message) throws IOException {
        return message; // Просто возвращаем полученные байты
    }

    @Override
    public boolean isEndOfStream(byte[] nextElement) {
        return false;
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return TypeInformation.of(byte[].class);
    }
}
