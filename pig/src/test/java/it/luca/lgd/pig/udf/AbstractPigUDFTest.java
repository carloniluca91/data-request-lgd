package it.luca.lgd.pig.udf;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@AllArgsConstructor
public abstract class AbstractPigUDFTest<T, R> {

    protected final Tuple tuple = Mockito.mock(Tuple.class);
    private final AbstractPigUDF<R> tAbstractPigUDF;
    protected final T[] inputValues;

    abstract protected void setMockingBehavior() throws ExecException;

    abstract protected R expectedFunction(T input);

    @Test
    public void testUDFExec() throws ExecException {

        setMockingBehavior();
        for (T inputValue: inputValues) {
            assertEquals(expectedFunction(inputValue), tAbstractPigUDF.exec(tuple));
        }
    }
}
