package it.luca.lgd.controller;

import it.luca.lgd.model.input.AbstractJobInput;
import it.luca.lgd.model.input.CiclilavStep1Input;
import it.luca.lgd.oozie.job.WorkflowJobId;
import it.luca.lgd.oozie.job.WorkflowJobParameter;
import it.luca.lgd.service.DRLGDService;
import it.luca.lgd.utils.TimeUtils;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.oozie.client.OozieClientException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

@Slf4j
@RestController
@AllArgsConstructor
@RequestMapping("/api/v1")
public class DRLGDController {

    @Autowired
    private final DRLGDService drlgdService;

    private <T extends AbstractJobInput> void runWorkflowJob(WorkflowJobId workflowJobId,
                                                             T abstractJobInput,
                                                             Function<T, Map<WorkflowJobParameter, String>> jobInputMapFunction)
            throws OozieClientException {

        if (abstractJobInput.isValid()) {
            log.info("Successsully validated input for workflow job '{}'. Parameters: {}", workflowJobId.getId(), abstractJobInput.toString());
            Map<WorkflowJobParameter, String> parameterStringMap = jobInputMapFunction.apply(abstractJobInput);
            drlgdService.runWorkflowJob(workflowJobId, parameterStringMap);
        } else {
            log.warn("Invalid input");
        }
    }

    @GetMapping("/ciclilavstep1")
    public void runCiclilavStep1(@Valid @RequestBody CiclilavStep1Input ciclilavStep1Input) {

        try {

            Function<CiclilavStep1Input, Map<WorkflowJobParameter, String>> inputMapFunction = input ->
                    new HashMap<WorkflowJobParameter, String>(){{
                        put(WorkflowJobParameter.START_DATE, input.getStartDate());
                        put(WorkflowJobParameter.END_DATE, input.getEndDate());
            }};

            runWorkflowJob(WorkflowJobId.CICLILAV_STEP1, ciclilavStep1Input, inputMapFunction);

        } catch (Exception e) {
            log.error("Error while trying to run workflow job '{}'. Stack trace: ", WorkflowJobId.CICLILAV_STEP1, e);
        }
    }
}