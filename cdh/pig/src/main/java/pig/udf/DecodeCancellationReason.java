package pig.udf;

import org.apache.pig.data.Tuple;

public class DecodeCancellationReason extends PigEvalFunction<String> {

    // Cases
    public static final String A = "A";
    public static final String B = "B";
    public static final String C = "C";
    public static final String D = "D";

    // Return values
    public static final String A_VALUE = "Airline/Carrier";
    public static final String B_VALUE = "Weather";
    public static final String C_VALUE = "National Air System";
    public static final String D_VALUE = "Security";

    @Override
    protected String processTuple(Tuple tuple) throws Exception {

        String outputString;
        String tuple0 = (String) tuple.get(0);
        switch (tuple0) {
            case A: outputString  = A_VALUE; break;
            case B: outputString = B_VALUE; break;
            case C: outputString = C_VALUE; break;
            case D: outputString = D_VALUE; break;
            default: outputString = null;
        }

        return outputString;
    }
}
