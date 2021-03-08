package it.luca.lgd.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonUtils {

    private final static ObjectMapper objectMapper = new ObjectMapper();

    public static <T> String objToString(T object) {

        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            log.error("Caught {} exception. Stack trace: ", e.getClass().getSimpleName(), e);
            return null;
        }
    }

    public static <T> T stringToObj(String string, Class<T> tClass) {

        try {
            String tClassName = tClass.getName();
            log.info("Deserializing JSON string to object of type {}", tClassName);
            T t = objectMapper
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .readValue(string, tClass);
            log.info("Deserialized JSON string to object of type {}", tClassName);
            return t;
        } catch (JsonProcessingException e) {
            log.error("Caught {} exception. Stack trace: ", e.getClass().getSimpleName(), e);
            return null;
        }
    }
}
