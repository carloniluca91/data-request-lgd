package it.luca.lgd.model;

import it.luca.lgd.oozie.WorkflowJobParameter;
import it.luca.lgd.utils.TimeUtils;
import it.luca.lgd.utils.Tuple2;
import lombok.*;

import javax.validation.constraints.NotBlank;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static it.luca.lgd.utils.TimeUtils.localDateToString;
import static it.luca.lgd.utils.TimeUtils.toLocalDate;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MonthlyGroupedDelaysParameters extends JobParameters {

    @NotBlank
    private String startMonth;

    @NotBlank
    private String endMonth;

    @NotBlank
    private List<String> airlines;

    @Override
    public Tuple2<Boolean, String> validate() {

        String startDate = String.format("%s-01", startMonth);
        String endDate = localDateToString(toLocalDate(String.format("%s-01", endMonth), DEFAULT_DATE_FORMAT)
                .plusMonths(1).minusDays(1), DEFAULT_DATE_FORMAT);

        return TimeUtils.isValidDate(startDate, DEFAULT_DATE_FORMAT) ?
                TimeUtils.isValidDate(endDate, DEFAULT_DATE_FORMAT) ?
                        TimeUtils.isBeforeOrEqual(startDate, endDate, DEFAULT_DATE_FORMAT) ?
                                new Tuple2<>(true, null) :
                                // StartDate greater than endDate
                                new Tuple2<>(false, String.format("%s (%s) is greater than %s (%s)",
                                        WorkflowJobParameter.START_DATE.getName(), startDate,
                                        WorkflowJobParameter.END_DATE.getName(), endDate)) :

                        // Invalid endDate
                        new Tuple2<>(false, String.format("Invalid %s (%s). It should follow format '%s'",
                                WorkflowJobParameter.END_DATE.getName(), endDate, DEFAULT_DATE_FORMAT)) :

                // Invalid startDate
                new Tuple2<>(false, String.format("Invalid %s (%s). It should follow format '%s'",
                        WorkflowJobParameter.START_DATE.getName(), startDate, DEFAULT_DATE_FORMAT));
    }

    @Override
    public Map<WorkflowJobParameter, String> toMap() {

        String endDate = localDateToString(toLocalDate(String.format("%s-01", endMonth), DEFAULT_DATE_FORMAT)
                .plusMonths(1).minusDays(1), DEFAULT_DATE_FORMAT);

        return new HashMap<WorkflowJobParameter, String>() {{
            put(WorkflowJobParameter.START_DATE, String.format("%s-01", startMonth));
            put(WorkflowJobParameter.END_DATE, endDate);
            put(WorkflowJobParameter.AIRLINE_IATAS, String.join(",", airlines));
        }};
    }
}
