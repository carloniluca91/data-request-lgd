package it.luca.lgd.pig.udf;

import org.apache.pig.backend.executionengine.ExecException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static it.luca.lgd.pig.udf.DecodeCancellationReason.*;

@RunWith(MockitoJUnitRunner.class)
public class DecodeCancellationReasonTest extends AbstractPigUDFTest<String, String> {

    public DecodeCancellationReasonTest() {
        super(new DecodeCancellationReason(), new String[]{A, B, C, D, "E"});
    }

    @Override
    protected void setMockingBehavior() throws ExecException {

        Mockito.when(tuple.get(0)).thenReturn(A, B, C, D, "E");
    }

    @Override
    protected String expectedFunction(String input) {

        String output;
        switch (input) {
            case A: output = A_VALUE; break;
            case B: output = B_VALUE; break;
            case C: output = C_VALUE; break;
            case D: output = D_VALUE; break;
            default: output = null; break;
        }

        return output;
    }
}