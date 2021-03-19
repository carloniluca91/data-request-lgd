package pig.udf;

import org.apache.pig.backend.executionengine.ExecException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class IsInTest extends PigEvalFunctionTest<String, Boolean> {

    private static final String FIRST = "first";
    private static final String SECOND = "second";
    private static final String THIRD = "third";

    public IsInTest() {
        super(new IsIn(), new String[]{FIRST, SECOND, THIRD});
    }

    @Override
    protected void setMockingBehavior() throws ExecException {

        when(tuple.get(0)).thenReturn(FIRST, SECOND, THIRD);
        when(tuple.get(1)).thenReturn(String.format("%s,%s", FIRST, SECOND));
    }

    @Override
    protected Boolean expectedFunction(String input) {

        return Arrays.asList(FIRST, SECOND).contains(input);
    }
}