package pig.udf;

import org.apache.pig.backend.executionengine.ExecException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static org.mockito.Mockito.when;

public class ToDateTest extends PigEvalFunctionTest<String, DateTime> {

    private final static String PATTERN = "yyyy-MM-dd HH:mm";
    private final static String GOOD_TIME = "2021-01-01 23:30";
    private final static String BAD_TIME = "2021-01-01 24:00";

    public ToDateTest() {
        super(new ToDate(), new String[]{GOOD_TIME, BAD_TIME});
    }

    @Override
    protected void setMockingBehavior() throws ExecException {

        when(tuple.get(0)).thenReturn(GOOD_TIME, BAD_TIME);
        when(tuple.get(1)).thenReturn(PATTERN);
    }

    @Override
    protected DateTime expectedFunction(String input) {

        int hourIndex = PATTERN.indexOf("HH");
        String hour = input.substring(hourIndex, hourIndex + 2);
        DateTimeFormatter formatter = DateTimeFormat.forPattern(PATTERN);
        return hour.equals("24") ?
                formatter.parseDateTime(input.substring(0, hourIndex) + "00" + input.substring(hourIndex + 2)).plusDays(1) :
                formatter.parseDateTime(input);
    }
}