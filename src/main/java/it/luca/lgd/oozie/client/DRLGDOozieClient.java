package it.luca.lgd.oozie.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import it.luca.lgd.oozie.job.WorkflowJobId;
import it.luca.lgd.oozie.job.WorkflowJobParameter;
import it.luca.lgd.oozie.json.WorkflowJobJsonSerializer;
import it.luca.lgd.oozie.utils.JobConfiguration;
import it.luca.lgd.oozie.utils.JobProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

import java.util.Map;

@SuppressWarnings("BusyWait")
@Slf4j
public class DRLGDOozieClient extends OozieClient {

    private final JobConfiguration jobConfiguration = new JobConfiguration("job.properties");
    private final ObjectMapper objectMapper = new ObjectMapper();

    public DRLGDOozieClient(String url) throws ConfigurationException {

        super(url);

        // Initialize ObjectMapper
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(WorkflowJob.class, new WorkflowJobJsonSerializer());
        objectMapper.registerModule(simpleModule);
        log.info("Successfully connected to Oozie Url {}", url);
    }

    public void runWorkflowJob(WorkflowJobId workflowJobId, Map<WorkflowJobParameter, String> parameterMap) throws Exception {

        log.info("Received a request for executing workflow jobId '{}'", workflowJobId.getId());

        // Initialize job properties
        JobProperties jobProperties = JobProperties.copyOf(jobConfiguration);

        // Set specific workflow job parameters ...
        jobProperties.setParameters(parameterMap);

        // ... and properties (workflow job name, workflow job HDFS path, pig script HDFS path)
        jobProperties.setParameter(WorkflowJobParameter.WORKFLOW_NAME, String.format("DataRequestLGD - %s", workflowJobId.getId()));
        WorkflowJobParameter oozieWfPath, pigScriptPath;
        switch (workflowJobId) {
            case CICLILAV_STEP1:
                oozieWfPath = WorkflowJobParameter.CICLILAV_STEP1_WORKFLOW;
                pigScriptPath = WorkflowJobParameter.CICLILAV_STEP1_PIG;
                break;

            case FPASPERD:
                oozieWfPath = WorkflowJobParameter.FPASPERD_WORKFLOW;
                pigScriptPath = WorkflowJobParameter.FPASPERD_PIG;
                break;

            default:
                log.warn("Undefined workflow jobId '{}'. Thus, nothing will be executed", workflowJobId.getId());
                return;
        }

        jobProperties.setParameter(WorkflowJobParameter.WORKFLOW_PATH, jobConfiguration.getParameter(oozieWfPath));
        jobProperties.setParameter(WorkflowJobParameter.PIG_SCRIPT_PATH, jobConfiguration.getParameter(pigScriptPath));
        log.info("Provided properties for workflow job '{}': {}", workflowJobId.getId(), jobProperties.getPropertiesReport());

        // Run and monitor workflow job
        String oozieWorkflowJobId = super.run(jobProperties);
        log.info("Workflow job '{}' submitted ({})", workflowJobId.getId(), oozieWorkflowJobId);
        monitorWorkflowJobExecution(oozieWorkflowJobId);
    }

    private void monitorWorkflowJobExecution(String workflowJobId) throws OozieClientException, JsonProcessingException, InterruptedException {

        // Wait until the workflow job finishes printing the status every N seconds
        int POLLING_SECONDS = 5;
        log.info("Workflow job '{}' is running. Information(s) on its execution will be polled every {} second(s)", workflowJobId, POLLING_SECONDS);
        while (super.getJobInfo(workflowJobId).getStatus() == WorkflowJob.Status.RUNNING) {

            Thread.sleep(POLLING_SECONDS * 1000);
            log.info("Workflow job report: {}", objectMapper.writeValueAsString(super.getJobInfo(workflowJobId)));
        }

        // Final workflow job report
        WorkflowJob workflowJob =  super.getJobInfo(workflowJobId);
        log.info("Final report for workflow job '{}':\n{}\n", workflowJobId, objectMapper.writeValueAsString(workflowJob));
        if (workflowJob.getStatus() == WorkflowJob.Status.SUCCEEDED) {
            log.info("Successfully executed workflow job '{}'", workflowJobId);
        } else {
            log.warn("Unable to fully execute workflow job '{}'", workflowJobId);
        }
    }
}
