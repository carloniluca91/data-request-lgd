package pig.udf;

import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class ToDate extends PigEvalFunction<DateTime>{

    @Override
    protected DateTime processTuple(Tuple tuple) throws Exception {

        String date = (String) tuple.get(0);
        String pattern = (String) tuple.get(1);

        // Extract hour from input date
        int hourSubstringIndex = pattern.indexOf("HH");
        String hour = date.substring(hourSubstringIndex, hourSubstringIndex + 2);
        DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern);
        return hour.equals("24") ?
                formatter.parseDateTime(date.substring(0, hourSubstringIndex) + "00" + date.substring(hourSubstringIndex + 2)).plusDays(1) :
                formatter.parseDateTime(date);
    }
}
