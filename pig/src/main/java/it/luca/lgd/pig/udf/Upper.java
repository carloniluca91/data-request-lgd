package it.luca.lgd.pig.udf;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class Upper extends AbstractPigUDF<String> {

    @Override
    protected String processTuple(Tuple tuple) throws ExecException {
        return ((String) tuple.get(0)).toUpperCase();
    }
}
