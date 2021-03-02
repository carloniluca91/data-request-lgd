package it.luca.lgd.controller;

import it.luca.lgd.jdbc.record.OozieActionRecord;
import it.luca.lgd.jdbc.record.OozieJobRecord;
import it.luca.lgd.model.parameters.CiclilavStep1Parameters;
import it.luca.lgd.model.parameters.JobParameters;
import it.luca.lgd.model.response.WorkflowJobResponse;
import it.luca.lgd.oozie.WorkflowJobId;
import it.luca.lgd.service.DRLGDService;
import it.luca.lgd.utils.Tuple2;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/v1")
public class DRLGDController {

    @Autowired
    private DRLGDService drlgdService;

    /*
     *************************
     * OOZIE JOBS SUBMISSION *
     *************************
     */

    private <T extends JobParameters> WorkflowJobResponse<T> runOozieJob(T jobParameters) {

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

    @PostMapping("/submit/ciclilavstep1")
    public WorkflowJobResponse<CiclilavStep1Parameters> runCiclilavStep1(@Valid @RequestBody CiclilavStep1Parameters ciclilavStep1Parameters) {

        String workflowJobId = ciclilavStep1Parameters.getWorkflowJobId().getId();
        log.info("Received a request for running workflow job '{}'", workflowJobId);
        WorkflowJobResponse<CiclilavStep1Parameters> workflowJobResponse = runOozieJob(ciclilavStep1Parameters);
        log.info("Successfully retrieved {}<{}> for workflow job '{}'",
                WorkflowJobResponse.class.getName(), CiclilavStep1Parameters.class.getName(), workflowJobId);
        return workflowJobResponse;
    }

    /*
    *************************
    * OOZIE JOBS MONITORING *
    *************************
    */

    @GetMapping("/jobs/status")
    public OozieJobRecord getOozieJob(@RequestParam("id") String workflowJobId) {

        String className = OozieJobRecord.class.getName();
        log.info("Received a request for getting {} for workflow job '{}'", className, workflowJobId);
        OozieJobRecord oozieJobRecord = drlgdService.getOozieJobStatusById(workflowJobId);
        log.info("Successfully retrieved {} for workflow job '{}'", className, workflowJobId);
        return oozieJobRecord;
    }

    @GetMapping("/jobs/last")
    public List<OozieJobRecord> getLastNOozieJobs(@Valid @RequestParam(name = "n", required = false, defaultValue = "1") Integer n) {

        String className = OozieJobRecord.class.getName();
        log.info("Received a request for getting {} of last {} workflow job(s)", className, n);
        List<OozieJobRecord> oozieJobRecords = drlgdService.getLastOozieJobs(n);
        log.info("Successfully retrieved {} of last {} (found {}) workflow job(s)", className, n, oozieJobRecords.size());
        return oozieJobRecords;
    }

    /*
     ********************************
     * OOZIE JOB ACTIONS MONITORING *
     ********************************
     */

    @GetMapping("/actions")
    public List<OozieActionRecord> getOozieJobActions(@RequestParam("id") String workflowJobId) {

        String className = OozieActionRecord.class.getName();
        log.info("Received a request for getting all of {}(s) workflow job '{}'", className, workflowJobId);
        List<OozieActionRecord> oozieActionRecords = drlgdService.getOozieJobActions(workflowJobId);
        log.info("Successfully retrieved {} {}(s) related to workflow job '{}'", className, oozieActionRecords.size(), workflowJobId);
        return oozieActionRecords;
    }
}