package it.luca.lgd.pig.udf;

import org.apache.pig.backend.executionengine.ExecException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UpperTest extends AbstractPigUDFTest<String, String> {

    public UpperTest() {
        super(new Upper(), new String[]{"aWord"});
    }

    @Override
    protected void setMockingBehavior() throws ExecException {
        Mockito.when(tuple.get(0)).thenReturn(inputValues[0]);
    }

    @Override
    protected String expectedFunction(String input) {
        return input.toUpperCase();
    }
}