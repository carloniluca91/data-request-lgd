package it.luca.lgd.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class Upper extends EvalFunc<String> {

    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null) {
            return null;
        } else {
            try {
                return ((String) input.get(0)).toUpperCase();
            } catch (Exception e) {
                return null;
            }
        }
    }
}
