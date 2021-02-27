package it.luca.lgd.model.input;

import it.luca.lgd.oozie.job.WorkflowJobId;
import it.luca.lgd.oozie.job.WorkflowJobParameter;
import it.luca.lgd.utils.TimeUtils;
import lombok.Getter;

import javax.validation.constraints.NotBlank;

@Getter
public class CiclilavStep1Input extends AbstractJobInput {

    @NotBlank private final String startDate;
    @NotBlank private final String endDate;

    public CiclilavStep1Input(String startDate, String endDate) {

        super(WorkflowJobId.CICLILAV_STEP1);
        this.startDate = startDate;
        this.endDate = endDate;
    }

    @Override
    public boolean isValid() {

        return TimeUtils.isValidDate(startDate, "yyyy-MM-dd") &&
                TimeUtils.isValidDate(endDate, "yyyy-MM-dd") &&
                TimeUtils.isBeforeOrEqual(startDate, endDate, "yyyy-MM-dd");
    }

    @Override
    protected String asString() {
        return String.format("%s: '%s', %s: '%s'",
                WorkflowJobParameter.START_DATE.getName(), startDate,
                WorkflowJobParameter.END_DATE.getName(), endDate);
    }
}
