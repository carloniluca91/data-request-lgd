package it.luca.lgd.jdbc.record;

import it.luca.lgd.oozie.WorkflowJobLabel;
import it.luca.lgd.utils.Tuple2;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RequestRecordTest {

    @Test
    public void fromTuple2() {

        String OK_MSG = "That's right", KO_MSG = "Try again";

        Tuple2<Boolean, String> trueTuple = new Tuple2<>(true, OK_MSG);
        RequestRecord okResponse = RequestRecord.from(WorkflowJobLabel.FPASPERD, null, trueTuple);
        assertEquals(okResponse.getJobSubmissionCode(), RequestRecord.OK);
        assertEquals(okResponse.getJobLauncherId(), OK_MSG);
        assertNull(okResponse.getJobSubmissionError());

        Tuple2<Boolean, String> falseTuple = new Tuple2<>(false, KO_MSG);
        RequestRecord koResponse = RequestRecord.from(WorkflowJobLabel.FPASPERD, null, falseTuple);
        assertEquals(koResponse.getJobSubmissionCode(), RequestRecord.KO);
        assertEquals(koResponse.getJobSubmissionError(), KO_MSG);
        assertNull(koResponse.getJobLauncherId());
    }
}