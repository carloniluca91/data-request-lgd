package it.luca.lgd.oozie.client;

import it.luca.lgd.oozie.job.WorkflowJobId;
import it.luca.lgd.oozie.job.WorkflowJobParameter;
import it.luca.lgd.oozie.utils.JobConfiguration;
import it.luca.lgd.oozie.utils.JobProperties;
import it.luca.lgd.yarn.application.OozieLauncherSearchCriteria;
import it.luca.lgd.yarn.application.PigApplicationSearchCriteria;
import it.luca.lgd.yarn.client.DRLGDYarnClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;

import java.util.Map;
import java.util.Optional;

@Slf4j
public class DRLGDOozieClient extends OozieClient {

    private final JobConfiguration jobConfiguration = new JobConfiguration("job.properties");
    private final DRLGDYarnClient drlgdYarnClient;
    private ApplicationId oozieLauncherId;
    private ApplicationId pigApplicationId;

    public DRLGDOozieClient(String url) throws ConfigurationException {

        super(url);
        drlgdYarnClient = new DRLGDYarnClient(jobConfiguration.getString("resourceManagerHostName"));
        log.info("Successfully connected to Oozie Url {} and initialized {} instance", url, DRLGDOozieClient.class.getSimpleName());
    }

    public void runWorkflowJob(WorkflowJobId workflowJobId, Map<WorkflowJobParameter, String> parameterMap) throws Exception {

        log.info("Received a request for executing workflow jobId '{}'", workflowJobId.getId());

        // Initialize job properties, set workflow job name
        JobProperties jobProperties = JobProperties.copyOf(jobConfiguration);
        String workflobJobName = String.format("DataRequestLGD - %s Wf", workflowJobId.getId());
        jobProperties.setProperty(WorkflowJobParameter.WORKFLOW_NAME, workflobJobName);

        // Set specific workflow job properties and parameters
        WorkflowJobParameter oozieWfHDFSPath;
        WorkflowJobParameter pigScriptHDFSPath;
        switch (workflowJobId) {
            case CICLILAV_STEP1:

                oozieWfHDFSPath = WorkflowJobParameter.CICLILAV_STEP1_WORKFLOW_PATH;
                pigScriptHDFSPath = WorkflowJobParameter.CICLILAV_STEP1_PIG_SCRIPT_PATH;
                jobProperties.setParameter(WorkflowJobParameter.START_DATE, parameterMap);
                jobProperties.setParameter(WorkflowJobParameter.END_DATE, parameterMap);
                break;

            default:
                log.warn("Undefined workflow jobId ({}). Thus, nothing will be executed", workflowJobId.getId());
                return;
        }

        String pigScriptPath = jobConfiguration.getString(pigScriptHDFSPath);
        jobProperties.setProperty(OozieClient.APP_PATH, jobConfiguration.getString(oozieWfHDFSPath));
        jobProperties.setProperty(WorkflowJobParameter.PIG_SCRIPT, pigScriptPath);
        log.info("Provided properties for workflow job '{}': {}", workflowJobId.getId(), jobProperties.getPropertiesReport());

        String userName = jobConfiguration.getString(OozieClient.USER_NAME);
        String yarnQueue = jobConfiguration.getString("yarnQueue");
        OozieLauncherSearchCriteria oozieLauncherCriteria = new OozieLauncherSearchCriteria(userName, yarnQueue, workflobJobName);
        PigApplicationSearchCriteria pigApplicationSearchCriteria = new PigApplicationSearchCriteria(userName, yarnQueue, new Path(pigScriptPath).getName());

        // Submit and start the workflow job
        String oozieWorkflowJobId = super.run(jobProperties);
        log.info("Workflow job '{}' ({}) submitted", workflowJobId.getId(), oozieWorkflowJobId);
        this.monitorExecution(oozieWorkflowJobId, oozieLauncherCriteria, pigApplicationSearchCriteria);
    }

    @SuppressWarnings("BusyWait")
    private void monitorExecution(String oozieWorkflowJobId,
                                  OozieLauncherSearchCriteria oozieLauncherCriteria,
                                  PigApplicationSearchCriteria pigApplicationSearchCriteria) throws Exception {

        // Wait until the workflow job finishes printing the status every N seconds
        int POLLING_SECONDS = 5;
        while (super.getJobInfo(oozieWorkflowJobId).getStatus() == WorkflowJob.Status.RUNNING) {

            log.info("Workflow job '{}' running. Status info will be polled every {} second(s)", oozieWorkflowJobId, POLLING_SECONDS);
            if (Optional.ofNullable(oozieLauncherId).isPresent()) {
                drlgdYarnClient.pollApplicationReport(oozieLauncherId);
            } else { oozieLauncherId = drlgdYarnClient.getApplicationId(oozieLauncherCriteria); }

            if (Optional.ofNullable(pigApplicationId).isPresent()) {
                drlgdYarnClient.pollApplicationReport(pigApplicationId);
            } else { pigApplicationId = drlgdYarnClient.getApplicationId(pigApplicationSearchCriteria); }

            Thread.sleep(POLLING_SECONDS * 1000);
        }

        log.info("Workflow job '{}' completed", oozieWorkflowJobId);

        // Print final status of both workflow job and Pig application
        drlgdYarnClient.pollApplicationReport(oozieLauncherId);
        drlgdYarnClient.pollApplicationReport(pigApplicationId);
    }
}
