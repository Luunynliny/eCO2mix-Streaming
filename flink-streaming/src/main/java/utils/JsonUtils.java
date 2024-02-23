package utils;

import Dto.Consumption;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtils {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String convertConsumptionToJson(Consumption consumption) {
        try {
            return objectMapper.writeValueAsString(consumption);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }
}
