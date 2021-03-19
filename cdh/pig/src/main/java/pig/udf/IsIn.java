package pig.udf;

import org.apache.pig.data.Tuple;

import java.util.Arrays;

public class IsIn extends PigEvalFunction<Boolean> {

    @Override
    protected Boolean processTuple(Tuple tuple) throws Exception {

        String input = (String) tuple.get(0);
        String list = (String) tuple.get(1);
        return Arrays.asList(list.split(",")).contains(input);
    }
}
