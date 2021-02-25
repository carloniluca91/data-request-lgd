package it.luca.lgd.pig.udf;

import lombok.AllArgsConstructor;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@AllArgsConstructor
public abstract class AbstractPigUDFTest<T> {

    private final Tuple tuple = Mockito.mock(Tuple.class);
    private final AbstractPigUDF<T> tAbstractPigUDF;
    private final Function<T, T> ttFunction;
    private final T tMockValue;

    @Test
    public void testUDFExec() throws ExecException {

        when(tuple.get(0)).thenReturn(tMockValue);
        assertEquals(ttFunction.apply(tMockValue), tAbstractPigUDF.exec(tuple));
    }
}
