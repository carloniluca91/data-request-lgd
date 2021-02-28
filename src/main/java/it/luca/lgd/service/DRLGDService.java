package it.luca.lgd.service;

import it.luca.lgd.oozie.job.WorkflowJobId;
import it.luca.lgd.oozie.job.WorkflowJobParameter;
import it.luca.lgd.utils.JobConfiguration;
import it.luca.lgd.utils.JobProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
public class DRLGDService {

    @Value("${oozie.server.url}")
    private String oozieServerUrl;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private OozieClient startOozieClient() {

        // Start OozieClient and run workflow job
        OozieClient oozieClient = new OozieClient(oozieServerUrl);
        log.info("Successfully connected to Oozie Server Url {}", oozieServerUrl);
        return oozieClient;
    }

    public String runWorkflowJob(WorkflowJobId workflowJobId, Map<WorkflowJobParameter, String> parameterStringMap) {

        try {
            WorkflowJobParameter oozieWfPath, pigScriptPath;
            switch (workflowJobId) {
                case CICLILAV_STEP1:
                    oozieWfPath = WorkflowJobParameter.CICLILAV_STEP1_WORKFLOW;
                    pigScriptPath = WorkflowJobParameter.CICLILAV_STEP1_PIG;
                    break;

                default:
                    oozieWfPath = WorkflowJobParameter.FPASPERD_WORKFLOW;
                    pigScriptPath = WorkflowJobParameter.FPASPERD_PIG;
                    break;
            }

            // Initialize job properties
            JobConfiguration jobConfiguration = new JobConfiguration("job.properties");
            JobProperties jobProperties = JobProperties.copyOf(jobConfiguration);

            // ... and then set specific workflow job parameters/properties (workflow job name, workflow job HDFS path, pig script HDFS path)
            jobProperties.setParameters(parameterStringMap);
            jobProperties.setParameter(WorkflowJobParameter.WORKFLOW_NAME, String.format("DataRequestLGD - %s", workflowJobId.getId()));
            jobProperties.setParameter(WorkflowJobParameter.WORKFLOW_PATH, jobConfiguration.getParameter(oozieWfPath));
            jobProperties.setParameter(WorkflowJobParameter.PIG_SCRIPT_PATH, jobConfiguration.getParameter(pigScriptPath));
            log.info("Provided properties for workflow job '{}': {}", workflowJobId.getId(), jobProperties.getPropertiesReport());

            String oozieWorkflowJobId = startOozieClient().run(jobProperties);
            log.info("Workflow job '{}' submitted ({})", workflowJobId.getId(), oozieWorkflowJobId);
            return oozieWorkflowJobId;

        } catch (Exception e) {
            log.error("Unable to run workflow job '{}'. Stack trace: ", workflowJobId.getId(), e);
            return e.getMessage();
        }
    }

    public void monitorWorkflowJobExecution(String workflowJobId) {

        try {

            OozieClient oozieClient = startOozieClient();
            WorkflowJob workflowJob = oozieClient.getJobInfo(workflowJobId);
        } catch (Exception e) {

            log.warn("Caught an exception while trying to poll information about Oozie job '{}'. Stack trace: ", workflowJobId, e);
        }
    }
}
