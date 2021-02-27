package it.luca.lgd.service;

import it.luca.lgd.oozie.job.WorkflowJobId;
import it.luca.lgd.oozie.job.WorkflowJobParameter;
import it.luca.lgd.oozie.utils.JobConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Properties;

@Slf4j
@Service
public class DRLGDService {

    @Value("${oozie.server.url}")
    private String oozieServerUrl;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public String runWorkflowJob(WorkflowJobId workflowJobId, Map<WorkflowJobParameter, String> parameterStringMap) throws OozieClientException {

        switch (workflowJobId) {

            case CICLILAV_STEP1:

        }
        return new OozieClient(oozieServerUrl).run(new Properties());
    }
}
