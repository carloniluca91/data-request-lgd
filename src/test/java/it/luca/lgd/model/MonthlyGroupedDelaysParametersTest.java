package it.luca.lgd.model;

import it.luca.lgd.oozie.WorkflowJobParameter;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

import static it.luca.lgd.utils.TimeUtils.toLocalDate;
import static org.junit.jupiter.api.Assertions.*;

public class MonthlyGroupedDelaysParametersTest {

    private final String VALID_START_MONTH = "2020-11";
    private final String VALID_END_MONTH = "2020-12";
    private final List<String> AIRLINES = Arrays.asList("AA", "OO");

    @Test
    public void validate() {

        BiPredicate<String, String> biPredicate = (startMonth, endMonth) ->
                new MonthlyGroupedDelaysParameters(startMonth, endMonth, AIRLINES)
                        .validate().getT1();

        assertTrue(biPredicate.test(VALID_START_MONTH, VALID_END_MONTH));
        assertTrue(biPredicate.test(VALID_START_MONTH, VALID_START_MONTH));
        assertFalse(biPredicate.test(VALID_END_MONTH, VALID_START_MONTH));
        assertFalse(biPredicate.test("1991ab", VALID_END_MONTH));
    }

    @Test
    public void toMap() {

        MonthlyGroupedDelaysParameters monthlyGroupedDelaysParameters = new MonthlyGroupedDelaysParameters(
                VALID_START_MONTH, VALID_END_MONTH, AIRLINES);

        Map<WorkflowJobParameter, String> map = monthlyGroupedDelaysParameters.toMap();
        LocalDate expectedEndDate = toLocalDate(map.get(WorkflowJobParameter.END_DATE), JobParameters.DEFAULT_DATE_FORMAT);
        LocalDate actualEndDte = toLocalDate(String.format("%s-01", VALID_END_MONTH), JobParameters.DEFAULT_DATE_FORMAT)
                .plusMonths(1).minusDays(1);

        assertEquals(expectedEndDate, actualEndDte);
        assertTrue(map.containsKey(WorkflowJobParameter.AIRLINE_IATAS));
        assertEquals(AIRLINES.size(), map.get(WorkflowJobParameter.AIRLINE_IATAS).split(",").length);
    }
}