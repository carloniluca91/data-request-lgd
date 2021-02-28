package it.luca.lgd.model.response;

import it.luca.lgd.utils.Tuple2;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class WorkflowJobResponseTest {

    @Test
    public void fromTuple2() {

        String OK_MSG = "That's right", KO_MSG = "Try again";

        Tuple2<Boolean, String> trueTuple = new Tuple2<>(true, OK_MSG);
        WorkflowJobResponse<?> okResponse = WorkflowJobResponse.fromTuple2(null, trueTuple);
        assertEquals(okResponse.getJobSubmission(), WorkflowJobResponse.OK);
        assertEquals(okResponse.getOozieWorkflowJobId(), OK_MSG);
        assertNull(okResponse.getJobSubmissionError());

        Tuple2<Boolean, String> falseTuple = new Tuple2<>(false, KO_MSG);
        WorkflowJobResponse<?> koResponse = WorkflowJobResponse.fromTuple2(null, falseTuple);
        assertEquals(koResponse.getJobSubmission(), WorkflowJobResponse.KO);
        assertEquals(koResponse.getJobSubmissionError(), KO_MSG);
        assertNull(koResponse.getOozieWorkflowJobId());
    }
}