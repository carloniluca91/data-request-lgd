package pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public abstract class PigEvalFunction<T> extends EvalFunc<T> {

    protected abstract T processTuple(Tuple tuple) throws Exception;

    @Override
    public T exec(Tuple tuple) {
        try {
            return processTuple(tuple);
        } catch (Exception e) {
            return null;
        }
    }
}