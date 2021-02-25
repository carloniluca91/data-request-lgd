package it.luca.lgd.pig.udf;

import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UpperTest extends AbstractPigUDFTest<String> {

    public UpperTest() {
        super(new Upper(), String::toUpperCase, "aWord");
    }
}