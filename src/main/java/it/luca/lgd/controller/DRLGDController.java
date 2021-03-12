package it.luca.lgd.controller;

import it.luca.lgd.jdbc.record.OozieActionRecord;
import it.luca.lgd.jdbc.record.OozieJobRecord;
import it.luca.lgd.jdbc.record.RequestRecord;
import it.luca.lgd.model.parameters.CancelledFlightsParameters;
import it.luca.lgd.oozie.WorkflowJobId;
import it.luca.lgd.service.DRLGDService;
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

    @PostMapping("submit/cancelled_flights")
    public RequestRecord runCancelledFlightsJob(@Valid @RequestBody CancelledFlightsParameters parameters) {

        String workflowJobId = WorkflowJobId.CANCELLED_FLIGHTS.getId();
        log.info("Received a request for running Oozie job {}", workflowJobId);
        RequestRecord RequestRecord = drlgdService.runOozieJob(WorkflowJobId.CANCELLED_FLIGHTS, parameters);
        log.info("Successfully retrieved {} for Oozie job {}", RequestRecord.class.getSimpleName(), workflowJobId);
        return RequestRecord;
    }

    @GetMapping("/status")
    public OozieJobRecord findOozieJob(@RequestParam("id") String workflowJobId) {

        String className = OozieJobRecord.class.getSimpleName();
        log.info("Received a request for getting {} for Oozie job {}", className, workflowJobId);
        OozieJobRecord oozieJobRecord = drlgdService.findJob(workflowJobId);
        log.info("Successfully retrieved {} for Oozie job {}", className, workflowJobId);
        return oozieJobRecord;
    }

    @GetMapping("/actions")
    public List<OozieActionRecord> findOozieJobActions(@RequestParam("id") String workflowJobId) {

        String className = OozieActionRecord.class.getSimpleName();
        log.info("Received a request for getting all of {}(s) related to Oozie job {}", className, workflowJobId);
        List<OozieActionRecord> oozieActionRecords = drlgdService.findActionsForJob(workflowJobId);
        log.info("Successfully retrieved {} {}(s) related to Oozie job {}", className, oozieActionRecords.size(), workflowJobId);
        return oozieActionRecords;
    }
}