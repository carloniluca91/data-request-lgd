package it.luca.lgd.model;

import it.luca.lgd.oozie.WorkflowJobParameter;
import it.luca.lgd.utils.Tuple2;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotBlank;
import java.util.HashMap;
import java.util.Map;

import static it.luca.lgd.utils.TimeUtils.isBothStartDateAndEndDateValid;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FlightDetailsParameters extends JobParameters {

    @NotBlank
    private String startDate;

    @NotBlank
    private String endDate;

    @Override
    public Tuple2<Boolean, String> validate() {

        return isBothStartDateAndEndDateValid(startDate, endDate, DEFAULT_DATE_FORMAT);
    }

    @Override
    public Map<WorkflowJobParameter, String> toMap() {

        return new HashMap<WorkflowJobParameter, String>() {{
            put(WorkflowJobParameter.START_DATE, startDate);
            put(WorkflowJobParameter.END_DATE, endDate);
        }};
    }
}
