package it.luca.lgd.model.input;

import it.luca.lgd.oozie.job.WorkflowJobId;
import it.luca.lgd.oozie.job.WorkflowJobParameter;
import it.luca.lgd.utils.TimeUtils;
import it.luca.lgd.utils.Tuple2;
import lombok.Getter;

import javax.validation.constraints.NotBlank;
import java.util.HashMap;
import java.util.Map;

@Getter
public class CiclilavStep1Parameters extends JobParameters {

    @NotBlank private final String startDate;
    @NotBlank private final String endDate;

    public CiclilavStep1Parameters(String startDate, String endDate) {

        super(WorkflowJobId.CICLILAV_STEP1);
        this.startDate = startDate;
        this.endDate = endDate;
    }

    @Override
    public Tuple2<Boolean, String> areValid() {

        String defaultFormat = "yyyy-MM-dd";
        return TimeUtils.isValidDate(startDate, defaultFormat) ?
                TimeUtils.isValidDate(endDate, defaultFormat) ?
                        TimeUtils.isBeforeOrEqual(startDate, endDate, defaultFormat) ?
                                new Tuple2<>(true, null) :
                                // StartDate greater than endDate
                                new Tuple2<>(false, String.format("%s (%s) is greater than %s (%s)",
                                        WorkflowJobParameter.START_DATE.getName(), startDate,
                                        WorkflowJobParameter.END_DATE.getName(), endDate)) :

                        // Invalid endDate
                        new Tuple2<>(false, String.format("Invalid %s (%s). It should follow format '%s'",
                                WorkflowJobParameter.END_DATE.getName(), endDate, defaultFormat)) :

                // Invalid startDate
                new Tuple2<>(false, String.format("Invalid %s (%s). It should follow format '%s'",
                        WorkflowJobParameter.START_DATE.getName(), startDate, defaultFormat));
    }

    @Override
    public Map<WorkflowJobParameter, String> toMap() {

        return new HashMap<WorkflowJobParameter, String>() {{
            put(WorkflowJobParameter.START_DATE, startDate);
            put(WorkflowJobParameter.END_DATE, endDate);
        }};
    }

    @Override
    protected String asString() {

        return String.format("%s: '%s', %s: '%s'",
                WorkflowJobParameter.START_DATE.getName(), startDate,
                WorkflowJobParameter.END_DATE.getName(), endDate);
    }
}
