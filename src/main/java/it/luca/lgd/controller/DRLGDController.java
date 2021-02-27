package it.luca.lgd.controller;

import it.luca.lgd.oozie.job.WorkflowJobId;
import it.luca.lgd.service.DRLGDService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;

@Slf4j
@RestController
@AllArgsConstructor
@RequestMapping("/api/v1")
public class DRLGDController {

    @Autowired
    private final DRLGDService drlgdService;

    @GetMapping("/list")
    public List<WorkflowJobId> listAvailableJobs() {

        return Arrays.asList(WorkflowJobId.values());
    }
}