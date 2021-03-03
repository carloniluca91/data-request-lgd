package it.luca.lgd.pig.udf;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@AllArgsConstructor
public abstract class AbstractPigUDFTest<T> {

    protected final Tuple tuple = Mockito.mock(Tuple.class);
    private final AbstractPigUDF<T> tAbstractPigUDF;
    private final Function<T, T> ttFunction;
    protected final T[] tMockValues;

    protected void setMockingBehavior() throws ExecException {

        Mockito.when(tuple.get(0)).thenReturn(tMockValues[0]);
    }

    @Test
    public void testUDFExec() throws ExecException {

        setMockingBehavior();
        Arrays.stream(tMockValues)
                .forEach(t -> assertEquals(ttFunction.apply(t), tAbstractPigUDF.exec(tuple)));
    }
}
