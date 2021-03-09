package it.luca.lgd.model.parameters;

import it.luca.lgd.oozie.WorkflowJobParameter;
import it.luca.lgd.utils.TimeUtils;
import it.luca.lgd.utils.Tuple2;
import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.validation.constraints.NotBlank;
import java.util.HashMap;
import java.util.Map;

@Getter
@AllArgsConstructor
public class CancelledFlightsParameters implements JobParameters {

    @NotBlank
    private final String startDate;

    @NotBlank
    private final String endDate;

    @NotBlank
    private final String iataCode;

    @Override
    public Tuple2<Boolean, String> validate() {

        String DEFAULT_FORMAT = "yyyy-MM-dd";
        return TimeUtils.isValidDate(startDate, DEFAULT_FORMAT) ?
                TimeUtils.isValidDate(endDate, DEFAULT_FORMAT) ?
                        TimeUtils.isBeforeOrEqual(startDate, endDate, DEFAULT_FORMAT) ?
                                new Tuple2<>(true, null) :
                                // StartDate greater than endDate
                                new Tuple2<>(false, String.format("%s (%s) is greater than %s (%s)",
                                        WorkflowJobParameter.START_DATE.getName(), startDate,
                                        WorkflowJobParameter.END_DATE.getName(), endDate)) :

                        // Invalid endDate
                        new Tuple2<>(false, String.format("Invalid %s (%s). It should follow format '%s'",
                                WorkflowJobParameter.END_DATE.getName(), endDate, DEFAULT_FORMAT)) :

                // Invalid startDate
                new Tuple2<>(false, String.format("Invalid %s (%s). It should follow format '%s'",
                        WorkflowJobParameter.START_DATE.getName(), startDate, DEFAULT_FORMAT));
    }

    @Override
    public Map<WorkflowJobParameter, String> toMap() {

        return new HashMap<WorkflowJobParameter, String>() {{
            put(WorkflowJobParameter.START_DATE, startDate);
            put(WorkflowJobParameter.END_DATE, endDate);
            put(WorkflowJobParameter.IATA_CODE, iataCode);
        }};
    }
}
