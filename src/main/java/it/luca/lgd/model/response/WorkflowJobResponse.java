package it.luca.lgd.model.response;

import it.luca.lgd.model.input.JobParameters;
import it.luca.lgd.utils.Tuple2;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.ZonedDateTime;

@Getter
@AllArgsConstructor
public class WorkflowJobResponse<T extends JobParameters> {

    private final ZonedDateTime dataRequestTime = ZonedDateTime.now();
    private final T jobParameters;
    private final String jobSubmission;
    private final String jobSubmissionError;
    private final String oozieWorkflowJobId;

    public static <T extends JobParameters> WorkflowJobResponse<T> fromTuple2(T jobParameters, Tuple2<Boolean, String> tuple2) {

        return tuple2.getT1() ?
                new WorkflowJobResponse<>(jobParameters, "OK", null, tuple2.getT2()) :
                new WorkflowJobResponse<>(jobParameters, "KO", tuple2.getT2(), null);
    }
}
