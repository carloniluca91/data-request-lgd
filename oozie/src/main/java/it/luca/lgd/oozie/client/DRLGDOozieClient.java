package it.luca.lgd.oozie.client;

import it.luca.lgd.oozie.job.WorkflowJobId;
import it.luca.lgd.oozie.job.WorkflowJobParameter;
import it.luca.lgd.oozie.utils.JobConfiguration;
import it.luca.lgd.oozie.utils.JobProperties;
import it.luca.lgd.yarn.application.ApplicationSearchCriteria;
import it.luca.lgd.yarn.application.ApplicationType;
import it.luca.lgd.yarn.client.DRLGDYarnClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class DRLGDOozieClient extends OozieClient {

    public DRLGDOozieClient(String url) {

        super(url);
        log.info("Successfully connected to Oozie Url {}", url);
    }

    public void runWorkflowJob(WorkflowJobId workflowJobId, Map<WorkflowJobParameter, String> parameterMap) throws Exception {

        log.info("Received a request for executing workflow jobId '{}'", workflowJobId.getId());

        // Set common properties
        JobConfiguration jobConfiguration = new JobConfiguration();
        jobConfiguration.load(DRLGDOozieClient.class.getClassLoader().getResourceAsStream("job.properties"));
        JobProperties jobProperties = new JobProperties();
        //noinspection unchecked
        jobConfiguration.getKeys().forEachRemaining(o -> {
            String key = (String) o;
            jobProperties.setProperty(key, jobConfiguration.getString(key));
        });

        String yarnQueue = jobConfiguration.getString("yarnQueue");
        String workflobJobName = String.format("DataRequestLGD - %s Wf", workflowJobId.getId());
        jobProperties.setProperty(WorkflowJobParameter.WORKFLOW_NAME, workflobJobName);

        // Set specific workflow properties and parameters
        WorkflowJobParameter oozieWorkflowPath;
        WorkflowJobParameter pigScriptPath;
        switch (workflowJobId) {
            case CICLILAV_STEP1:

                oozieWorkflowPath = WorkflowJobParameter.CICLILAV_STEP1_WORKFLOW_PATH;
                pigScriptPath = WorkflowJobParameter.CICLILAV_STEP1_PIG_SCRIPT_PATH;
                jobProperties.setParameter(WorkflowJobParameter.START_DATE, parameterMap);
                jobProperties.setParameter(WorkflowJobParameter.END_DATE, parameterMap);
                break;

            default:
                log.warn("Undefined workflow jobId ({}). Thus, nothing will be executed", workflowJobId.getId());
                return;
        }

        String pigScriptHDFSPath = jobConfiguration.getString(pigScriptPath);
        jobProperties.setProperty(OozieClient.APP_PATH, jobConfiguration.getString(oozieWorkflowPath));
        jobProperties.setProperty(WorkflowJobParameter.PIG_SCRIPT, pigScriptHDFSPath);
        log.info("Provided properties for workflow job '{}': {}", workflowJobId.getId(), jobProperties.printProperties());

        DRLGDYarnClient drlgdYarnClient = new DRLGDYarnClient(jobConfiguration.getString("resourceManagerHostName"));
        ApplicationSearchCriteria oozieLauncherCriteria = new ApplicationSearchCriteria(jobConfiguration.getString(OozieClient.USER_NAME),
                ApplicationType.MAPREDUCE,
                s -> s.contains(workflobJobName),
                yarnQueue);

        String pigScriptName = new Path(pigScriptHDFSPath).getName();
        ApplicationSearchCriteria pigScriptCriteria = new ApplicationSearchCriteria(jobConfiguration.getString(OozieClient.USER_NAME),
                ApplicationType.MAPREDUCE,
                s -> s.equalsIgnoreCase(String.format("PigLatin:%s", pigScriptName)),
                yarnQueue);

        // Submit and start the workflow job
        String oozieWorkflowJobId = super.run(jobProperties);
        log.info("Workflow job '{}' ({}) submitted", workflowJobId.getId(), oozieWorkflowJobId);

        // Wait until the workflow job finishes printing the status every 10 seconds
        Optional<ApplicationId> optionalOozieLauncherAppId = Optional.empty();
        Optional<ApplicationId> optionalPigApplicationAppId = Optional.empty();
        List<YarnApplicationState> notCompleteStates = Arrays.asList(YarnApplicationState.SUBMITTED,
                YarnApplicationState.ACCEPTED,
                YarnApplicationState.RUNNING);

        while (super.getJobInfo(oozieWorkflowJobId).getStatus() == WorkflowJob.Status.RUNNING) {

            log.info("Workflow job '{}' ({}) running", workflowJobId.getId(), oozieWorkflowJobId);
            if (optionalOozieLauncherAppId.isPresent()) {
                log.info("vamos");
            } else {
                optionalOozieLauncherAppId = drlgdYarnClient.getApplicationId(oozieLauncherCriteria, notCompleteStates);
            }

            if (optionalPigApplicationAppId.isPresent()) {
                log.info("vamos");
            } else {
                optionalPigApplicationAppId = drlgdYarnClient.getApplicationId(pigScriptCriteria, notCompleteStates);
            }

            Thread.sleep(10 * 1000);
        }

        // print the final status o the workflow job
        log.info("Workflow job '{}' ({}) completed", workflowJobId.getId(), oozieWorkflowJobId);
        log.info("Successfully executed workflow job '{}'", workflowJobId.getId());
    }
}
