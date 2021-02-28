package it.luca.lgd.controller;

import it.luca.lgd.model.input.AbstractJobInput;
import it.luca.lgd.model.input.CiclilavStep1Input;
import it.luca.lgd.oozie.job.WorkflowJobId;
import it.luca.lgd.oozie.job.WorkflowJobParameter;
import it.luca.lgd.service.DRLGDService;
import it.luca.lgd.utils.Tuple2;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@Slf4j
@RestController
@AllArgsConstructor
@RequestMapping("/api/v1")
public class DRLGDController {

    @Autowired
    private final DRLGDService drlgdService;

    private <T extends AbstractJobInput> String runWorkflowJob(WorkflowJobId workflowJobId,
                                                               T abstractJobInput,
                                                               Function<T, Map<WorkflowJobParameter, String>> jobInputMapFunction) {

        Tuple2<Boolean, String> tuple2 = abstractJobInput.isValid();
        if (tuple2.getT1()) {

            // If provided input matches given criterium, run workflow job
            log.info("Successsully validated input for workflow job '{}'. Parameters: {}", workflowJobId.getId(), abstractJobInput.toString());
            Map<WorkflowJobParameter, String> parameterStringMap = jobInputMapFunction.apply(abstractJobInput);
            //return drlgdService.runWorkflowJob(workflowJobId, parameterStringMap);
            return "Vamos!";
        } else {

            // Otherwise, report the issue
            String invalidInputMessage = String.format("Invalid input for workflow job '%s'. Rationale: %s", workflowJobId.getId(), tuple2.getT2());
            log.warn(invalidInputMessage);
            return invalidInputMessage;
        }
    }

    @PostMapping("/ciclilavstep1")
    public String runCiclilavStep1(@Valid @RequestBody CiclilavStep1Input ciclilavStep1Input) {

        Function<CiclilavStep1Input, Map<WorkflowJobParameter, String>> inputMapFunction = input ->
                new HashMap<WorkflowJobParameter, String>(){{
                    put(WorkflowJobParameter.START_DATE, input.getStartDate());
                    put(WorkflowJobParameter.END_DATE, input.getEndDate());
        }};

        return runWorkflowJob(WorkflowJobId.CICLILAV_STEP1, ciclilavStep1Input, inputMapFunction);
    }
}