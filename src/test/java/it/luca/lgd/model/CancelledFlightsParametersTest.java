package it.luca.lgd.model;

import org.junit.jupiter.api.Test;

import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CancelledFlightsParametersTest {

    @Test
    public void areValid() {

        BiFunction<String, String, Boolean> biFunction = (start, end) ->
                new CancelledFlightsParameters(start, end, "ABE")
                        .validate().getT1();

        String VALID_START_DATE = "2020-12-31";
        String VALID_END_DATE = "2021-01-01";

        assertTrue(biFunction.apply(VALID_START_DATE, VALID_END_DATE));
        assertTrue(biFunction.apply(VALID_START_DATE, VALID_START_DATE));
        assertFalse(biFunction.apply("2020-1231", VALID_END_DATE));
        assertFalse(biFunction.apply(VALID_START_DATE, "2021-0101"));
        assertFalse(biFunction.apply(VALID_END_DATE, VALID_START_DATE));
    }
}