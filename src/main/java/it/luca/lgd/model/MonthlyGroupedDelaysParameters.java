package it.luca.lgd.model;

import it.luca.lgd.oozie.WorkflowJobParameter;
import it.luca.lgd.utils.Tuple2;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static it.luca.lgd.utils.TimeUtils.*;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MonthlyGroupedDelaysParameters extends JobParameters {

    @NotBlank
    private String startMonth;

    @NotBlank
    private String endMonth;

    @NotEmpty
    private List<String> airlines;

    @Override
    public Tuple2<Boolean, String> validate() {

        String startDate = String.format("%s-01", startMonth);
        String endDate = localDateToString(toLocalDate(String.format("%s-01", endMonth), DEFAULT_DATE_FORMAT)
                .plusMonths(1).minusDays(1), DEFAULT_DATE_FORMAT);

        return isBothStartDateAndEndDateValid(startDate, endDate, DEFAULT_DATE_FORMAT);
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
