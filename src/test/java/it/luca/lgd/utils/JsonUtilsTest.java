package it.luca.lgd.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class JsonUtilsTest {

    @Test
    public void stringToObject() {

        Integer a = 1;
        String b = "hello";
        String string = String.format("{\"a\": %s, \"b\": \"%s\"}", a, b);
        JsonBean bean = JsonUtils.stringToObject(string, JsonBean.class);
        assertNotNull(bean);
        assertEquals(a, bean.getA());
        assertEquals(b, bean.getB());
    }
}