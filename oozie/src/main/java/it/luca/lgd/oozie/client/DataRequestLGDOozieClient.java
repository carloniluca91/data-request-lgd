package it.luca.lgd.oozie.client;

import it.luca.lgd.oozie.job.JobId;
import it.luca.lgd.oozie.utils.JobConfiguration;
import it.luca.lgd.oozie.utils.JobProperties;
import it.luca.lgd.oozie.job.WorkflowJobParameter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

@Slf4j
public class DataRequestLGDOozieClient extends OozieClient {

    public DataRequestLGDOozieClient(String url) {

        super(url);
        log.info("Successfully connected to Oozie Url {}", url);
    }

    public void runWorkflowJob(JobId jobId) throws OozieClientException, InterruptedException, ConfigurationException {

        log.info("Received a request for executing workflowJob '{}'", jobId.getId());

        // Set common properties
        JobConfiguration jobConfiguration = new JobConfiguration();
        jobConfiguration.load(DataRequestLGDOozieClient.class.getClassLoader().getResourceAsStream("job.properties"));
        JobProperties jobProperties = new JobProperties();
        jobConfiguration.getKeys().forEachRemaining(k -> jobProperties.setProperty(k, jobConfiguration.getString(k)));
        jobProperties.setProperty(WorkflowJobParameter.WORKFLOW_NAME, String.format("DataRequestLGD - %s Wf", jobId.getId()));

        String oozieWorkflowPath;
        String pigScriptPath;
        switch (jobId) {

            // set specific workflow properties
            case CICLILAV_STEP1:

                oozieWorkflowPath = jobConfiguration.getString(WorkflowJobParameter.CICLILAV_STEP1_WORKFLOW_PATH);
                pigScriptPath = jobConfiguration.getString(WorkflowJobParameter.CICLILAV_STEP1_PIG_SCRIPT_PATH);

                jobProperties.setProperty(WorkflowJobParameter.START_DATE, "1991-01-01");
                jobProperties.setProperty(WorkflowJobParameter.END_DATE, "1991-12-31");
                break;

            default:
                log.warn("Undefined jobId ({}). Thus, nothing will be executed", jobId.getId());
                return;
        }

        jobProperties.setProperty(OozieClient.APP_PATH, oozieWorkflowPath);
        jobProperties.setProperty(WorkflowJobParameter.PIG_SCRIPT, pigScriptPath);
        log.info("Provided properties for workflowJob '{}': {}", jobId.getId(), jobProperties.printProperties());

        // submit and start the workflow job
        String workflowJobId = run(jobProperties);
        log.info("Workflow job {} submitted", workflowJobId);

        // wait until the workflow job finishes printing the status every 10 seconds
        while (getJobInfo(workflowJobId).getStatus() == WorkflowJob.Status.RUNNING) {
            log.info("Workflow job running ...");
            Thread.sleep(10 * 1000);
        }

        // print the final status o the workflow job
        log.info("Workflow job completed ...");
    }
}
