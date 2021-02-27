package it.luca.lgd.service;

import it.luca.lgd.oozie.job.WorkflowJobId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class DRLGDService {

    @Value("${oozie.url}")
    private String oozieServerUrl;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public void runWorkflowJob(WorkflowJobId workflowJobId, Map<String, String> jobParameters) {

    }
}
