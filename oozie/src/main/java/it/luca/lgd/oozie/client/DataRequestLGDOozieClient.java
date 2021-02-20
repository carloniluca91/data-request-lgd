package it.luca.lgd.oozie.client;

import it.luca.lgd.oozie.job.JobId;
import lombok.extern.slf4j.Slf4j;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

import java.util.Properties;

@Slf4j
public class DataRequestLGDOozieClient extends OozieClient {

    public DataRequestLGDOozieClient(String url) {

        super(url);
        log.info("Successfully connected to Oozie Url {}", url);
    }

    public void runWorkflowJob(JobId jobId) throws OozieClientException, InterruptedException {

        log.info("Received a request for executing workflowJob '{}'", jobId.getId());

        // Set common properties
        Properties jobProperties = new Properties();
        jobProperties.setProperty(OozieClient.USER_NAME, "");
        jobProperties.setProperty("jobTracker", "");
        jobProperties.setProperty("nameNode", "");

        switch (jobId) {

            // set specific workflow properties
            case CICLILAV_STEP1:

                jobProperties.setProperty(OozieClient.APP_PATH, "hdfs://foo:8020/usr/tucu/my-wf-app");
                break;

            default:
                log.warn("Undefined jobId ({}). Thus, nothing will be executed", jobId.getId());
                return;
        }

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
