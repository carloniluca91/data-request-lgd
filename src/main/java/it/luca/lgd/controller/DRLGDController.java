package it.luca.lgd.controller;

import it.luca.lgd.jdbc.record.OozieActionRecord;
import it.luca.lgd.jdbc.record.OozieJobRecord;
import it.luca.lgd.jdbc.record.RequestRecord;
import it.luca.lgd.model.CancelledFlightsParameters;
import it.luca.lgd.oozie.WorkflowJobLabel;
import it.luca.lgd.service.DRLGDService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/v1/job")
public class DRLGDController {

    @Autowired
    private DRLGDService drlgdService;

    /**
     * Run CANCELLED_FLIGHTS Oozie job
     * @param parameters: job parameters
     * @return RequestRecord reporting job submission outcome
     */

    @PostMapping("submit/cancelled_flights")
    public RequestRecord runCancelledFlightsJob(@Valid @RequestBody CancelledFlightsParameters parameters) {

        String workflowJobId = WorkflowJobLabel.CANCELLED_FLIGHTS.getId();
        log.info("Received a request for running Oozie job {}", workflowJobId);
        RequestRecord RequestRecord = drlgdService.runOozieJob(WorkflowJobLabel.CANCELLED_FLIGHTS, parameters);
        log.info("Successfully retrieved {} for Oozie job {}", RequestRecord.class.getSimpleName(), workflowJobId);
        return RequestRecord;
    }

    /**
     * Return OozieJobRecord for provided Oozie job id
     * @param workflowJobId: Oozie job id
     * @return OozieJobRecord
     */

    @GetMapping("/status")
    public OozieJobRecord findOozieJob(@RequestParam("id") String workflowJobId) {

        String className = OozieJobRecord.class.getSimpleName();
        log.info("Received a request for getting {} for Oozie job {}", className, workflowJobId);
        OozieJobRecord oozieJobRecord = drlgdService.findJob(workflowJobId);
        log.info("Successfully retrieved {} for Oozie job {}", className, workflowJobId);
        return oozieJobRecord;
    }

    /**
     * Return a list of OozieActionRecords for provided Oozie job id
     * @param workflowJobId: Oozie job id
     * @return a list of OozieActionRecords
     */

    @GetMapping("/actions")
    public List<OozieActionRecord> findOozieJobActions(@RequestParam("id") String workflowJobId) {

        String className = OozieActionRecord.class.getSimpleName();
        log.info("Received a request for getting all of {}(s) of Oozie job {}", className, workflowJobId);
        List<OozieActionRecord> oozieActionRecords = drlgdService.findActionsForJob(workflowJobId);
        log.info("Successfully retrieved {} {}(s) for Oozie job {}", className, oozieActionRecords.size(), workflowJobId);
        return oozieActionRecords;
    }
}