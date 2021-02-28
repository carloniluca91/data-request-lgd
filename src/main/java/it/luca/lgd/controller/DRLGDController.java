package it.luca.lgd.controller;

import it.luca.lgd.model.jdbc.WorkflowJobRecord;
import it.luca.lgd.model.parameters.CiclilavStep1Parameters;
import it.luca.lgd.model.parameters.JobParameters;
import it.luca.lgd.model.response.WorkflowJobResponse;
import it.luca.lgd.oozie.WorkflowJobId;
import it.luca.lgd.service.DRLGDService;
import it.luca.lgd.utils.Tuple2;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;

@Slf4j
@RestController
@AllArgsConstructor
@RequestMapping("/api/v1")
public class DRLGDController {

    @Autowired
    private final DRLGDService drlgdService;

    private <T extends JobParameters> WorkflowJobResponse<T> runWorkflowJob(T jobParameters) {

        Tuple2<Boolean, String> inputValidation = jobParameters.areValid();
        WorkflowJobId workflowJobId = jobParameters.getWorkflowJobId();
        if (inputValidation.getT1()) {

            // If provided input matches given criterium, run workflow job
            log.info("Successsully validated input for workflow job '{}'. Parameters: {}", workflowJobId.getId(), jobParameters.toString());
            return WorkflowJobResponse.fromTuple2(jobParameters, drlgdService.runWorkflowJob(workflowJobId, jobParameters.toMap()));
        } else {

            // Otherwise, report the issue
            String errorMsg = String.format("Invalid input for workflow job '%s'. Rationale: %s", workflowJobId.getId(), inputValidation.getT2());
            log.warn(errorMsg);
            return WorkflowJobResponse.fromTuple2(jobParameters, inputValidation);
        }
    }

    @PostMapping("/ciclilavstep1")
    public WorkflowJobResponse<CiclilavStep1Parameters> runCiclilavStep1(@Valid @RequestBody CiclilavStep1Parameters ciclilavStep1Parameters) {
        return runWorkflowJob(ciclilavStep1Parameters);
    }

    @GetMapping("/status")
    public WorkflowJobRecord getJobStatus(@RequestParam("id") String workflowJobId) {

        return drlgdService.monitorWorkflowJobExecution(workflowJobId);
    }
}