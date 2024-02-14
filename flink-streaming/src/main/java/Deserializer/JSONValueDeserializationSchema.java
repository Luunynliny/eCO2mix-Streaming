package Deserializer;

import Dto.Consumption;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.json.JsonReadFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JSONValueDeserializationSchema implements DeserializationSchema<Consumption> {

    private final ObjectMapper objectMapper = new ObjectMapper().enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature());

    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public Consumption deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, Consumption.class);
    }

    @Override
    public boolean isEndOfStream(Consumption consumption) {
        return false;
    }

    @Override
    public TypeInformation<Consumption> getProducedType() {
        return TypeInformation.of(Consumption.class);
    }
}
