package it.luca.lgd.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonUtils {

    private final static ObjectMapper objectMapper = new ObjectMapper();
    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Serialized input object to JSON string
     * @param object: input object
     * @param <T>: input object type
     * @return JSON string (null if serialization fails)
     */

    public static <T> String objectToString(T object) {

        try {
            String className = object.getClass().getSimpleName();
            log.info("Serializing {} object to JSON string", className);
            String output = objectMapper.writeValueAsString(object);
            log.info("Serialized {} object to JSON string", className);
            return output;
        } catch (JsonProcessingException e) {
            log.error("Caught exception during JSON serialization. Stack trace: ", e);
            return null;
        }
    }

    /**
     * Deserialize input JSON string to an object
     * @param string: JSON string
     * @param tClass: return object class
     * @param <T> return object type
     * @return deserialized object (null if deserialization failed)
     */

    public static <T> T stringToObject(String string, Class<T> tClass) {

        try {
            String tClassName = tClass.getSimpleName();
            log.info("Deserializing JSON string to object of type {}", tClassName);
            T t = objectMapper.readValue(string, tClass);
            log.info("Deserialized JSON string to object of type {}", tClassName);
            return t;
        } catch (JsonProcessingException e) {
            log.error("Caught exception during JSON deserialization. Stack trace: ", e);
            return null;
        }
    }
}
