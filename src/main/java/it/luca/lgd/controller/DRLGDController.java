package it.luca.lgd.controller;

import it.luca.lgd.jdbc.record.OozieActionRecord;
import it.luca.lgd.jdbc.record.OozieJobRecord;
import it.luca.lgd.model.parameters.CancelledFlightsParameters;
import it.luca.lgd.model.parameters.JobParameters;
import it.luca.lgd.jdbc.record.RequestRecord;
import it.luca.lgd.oozie.WorkflowJobId;
import it.luca.lgd.service.DRLGDService;
import it.luca.lgd.utils.Tuple2;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/v1/job")
public class DRLGDController {

    @Autowired
    private DRLGDService drlgdService;

    /*
     *************************
     * OOZIE JOBS SUBMISSION *
     *************************
     */

    private <T extends JobParameters> RequestRecord runOozieJob(WorkflowJobId workflowJobId, T jobParameters) {

        Tuple2<Boolean, String> inputValidation = jobParameters.validate();
        RequestRecord requestRecord;
        if (inputValidation.getT1()) {

            // If provided input matches given criterium, run workflow job
            log.info("Successsully validated input for workflow job '{}'. Parameters: {}", workflowJobId.getId(), jobParameters.asString());
            requestRecord = RequestRecord.from(workflowJobId, jobParameters, drlgdService.runWorkflowJob(workflowJobId, jobParameters.toMap()));
        } else {

            // Otherwise, report the issue
            String errorMsg = String.format("Invalid input for workflow job '%s'. Rationale: %s", workflowJobId.getId(), inputValidation.getT2());
            log.warn(errorMsg);
            requestRecord = RequestRecord.from(workflowJobId, jobParameters, inputValidation);
        }

        int requestId = drlgdService.insertRequestRecord(requestRecord);
        requestRecord.setRequestId(requestId);
        return requestRecord;
    }

    @PostMapping("submit/cancelled_flights")
    public RequestRecord runCancelledFlightsJob(@Valid @RequestBody CancelledFlightsParameters parameters) {

        String workflowJobId = WorkflowJobId.CANCELLED_FLIGHTS.getId();
        log.info("Received a request for running workflow job '{}'", workflowJobId);
        RequestRecord RequestRecord = runOozieJob(WorkflowJobId.CANCELLED_FLIGHTS, parameters);
        log.info("Successfully retrieved {} for workflow job '{}'", RequestRecord.class.getSimpleName(), workflowJobId);
        return RequestRecord;
    }

    /*
    *************************
    * OOZIE JOBS MONITORING *
    *************************
    */

    @GetMapping("/status")
    public OozieJobRecord getOozieJob(@RequestParam("id") String workflowJobId) {

        String className = OozieJobRecord.class.getSimpleName();
        log.info("Received a request for getting {} for workflow job '{}'", className, workflowJobId);
        OozieJobRecord oozieJobRecord = drlgdService.getOozieJobStatus(workflowJobId);
        log.info("Successfully retrieved {} for workflow job '{}'", className, workflowJobId);
        return oozieJobRecord;
    }

    @GetMapping("/actions")
    public List<OozieActionRecord> getOozieJobActions(@RequestParam("id") String workflowJobId) {

        String className = OozieActionRecord.class.getSimpleName();
        log.info("Received a request for getting all of {}(s) related to workflow job '{}'", className, workflowJobId);
        List<OozieActionRecord> oozieActionRecords = drlgdService.getOozieJobActions(workflowJobId).stream()
                .sorted(Comparator.comparing(OozieActionRecord::getActionStartTime))
                .collect(Collectors.toList());
        log.info("Successfully retrieved {} {}(s) related to workflow job '{}'", className, oozieActionRecords.size(), workflowJobId);
        return oozieActionRecords;
    }
}