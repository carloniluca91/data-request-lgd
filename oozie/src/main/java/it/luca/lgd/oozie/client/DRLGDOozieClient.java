package it.luca.lgd.oozie.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import it.luca.lgd.oozie.job.WorkflowJobId;
import it.luca.lgd.oozie.job.WorkflowJobParameter;
import it.luca.lgd.oozie.json.WorkflowActionJsonSerializer;
import it.luca.lgd.oozie.json.WorkflowJobJsonSerializer;
import it.luca.lgd.oozie.utils.JobConfiguration;
import it.luca.lgd.oozie.utils.JobProperties;
import it.luca.lgd.yarn.application.OozieLauncherSearchCriteria;
import it.luca.lgd.yarn.application.PigApplicationSearchCriteria;
import it.luca.lgd.yarn.client.DRLGDYarnClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@SuppressWarnings("BusyWait")
@Slf4j
public class DRLGDOozieClient extends OozieClient {

    private final JobConfiguration jobConfiguration = new JobConfiguration("job.properties");
    private final ObjectMapper objectMapper = new ObjectMapper();
    private DRLGDYarnClient drlgdYarnClient;
    private ApplicationId oozieLauncherId;
    private ApplicationId pigApplicationId;

    public DRLGDOozieClient(String url) throws ConfigurationException {

        super(url);

        // Initialize ObjectMapper
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(WorkflowJob.class, new WorkflowJobJsonSerializer());
        simpleModule.addSerializer(WorkflowAction.class, new WorkflowActionJsonSerializer());
        objectMapper.registerModule(simpleModule);

        //drlgdYarnClient = new DRLGDYarnClient(jobConfiguration.getString("resourceManagerHostName"));
        log.info("Successfully connected to Oozie Url {} and initialized {} instance", url, DRLGDOozieClient.class.getSimpleName());
    }

    public void runWorkflowJob(WorkflowJobId workflowJobId, Map<WorkflowJobParameter, String> parameterMap) throws Exception {

        log.info("Received a request for executing workflow jobId '{}'", workflowJobId.getId());

        // Initialize job properties
        JobProperties jobProperties = JobProperties.copyOf(jobConfiguration);

        // Set specific workflow job properties (name, workflow path, pig script path) and parameters
        String workflobJobName = String.format("DataRequestLGD - %s Wf", workflowJobId.getId());
        jobProperties.setProperty(WorkflowJobParameter.WORKFLOW_NAME, workflobJobName);
        jobProperties.setParameters(parameterMap);

        WorkflowJobParameter oozieWfPath, pigScriptPath;
        switch (workflowJobId) {
            case CICLILAV_STEP1:

                oozieWfPath = WorkflowJobParameter.CICLILAV_STEP1_WORKFLOW;
                pigScriptPath = WorkflowJobParameter.CICLILAV_STEP1_PIG;
                break;

            case FPASPERD:

                oozieWfPath = WorkflowJobParameter.FPASPERD_WORKFLOW;
                pigScriptPath = WorkflowJobParameter.FPASPERD_WORKFLOW;
                break;

            default:
                log.warn("Undefined workflow jobId '{}'. Thus, nothing will be executed", workflowJobId.getId());
                return;
        }

        jobProperties.setProperty(OozieClient.APP_PATH, jobConfiguration.getString(oozieWfPath));
        jobProperties.setProperty(WorkflowJobParameter.PIG_SCRIPT_PATH, jobConfiguration.getString(pigScriptPath));
        log.info("Provided properties for workflow job '{}': {}", workflowJobId.getId(), jobProperties.getPropertiesReport());

        //String userName = jobConfiguration.getString(OozieClient.USER_NAME);
        //String yarnQueue = jobConfiguration.getString("yarnQueue");
        //OozieLauncherSearchCriteria oozieLauncherCriteria = new OozieLauncherSearchCriteria(userName, yarnQueue, workflobJobName);
        //PigApplicationSearchCriteria pigApplicationSearchCriteria = new PigApplicationSearchCriteria(userName, yarnQueue, new Path(pigScriptPath).getName());

        // Submit and start the workflow job
        String oozieWorkflowJobId = super.run(jobProperties);
        log.info("Workflow job '{}' ({}) submitted", workflowJobId.getId(), oozieWorkflowJobId);
        this.monitorWorkflowJobExecution(oozieWorkflowJobId);
    }

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

        log.info("Workflow job '{}' completed with status {}", oozieWorkflowJobId, super.getJobInfo(oozieWorkflowJobId).getStatus());

        // Print final status of both workflow job and Pig application
        drlgdYarnClient.pollApplicationReport(oozieLauncherId);
        drlgdYarnClient.pollApplicationReport(pigApplicationId);
    }

    private void monitorWorkflowJobExecution(String oozieWorkflowJobId) throws OozieClientException, JsonProcessingException, InterruptedException {

        // Wait until the workflow job finishes printing the status every N seconds
        int POLLING_SECONDS = 5;
        List<WorkflowAction.Status> completeStatuses = Arrays.asList(WorkflowAction.Status.FAILED,
                WorkflowAction.Status.KILLED,
                WorkflowAction.Status.DONE);

        Predicate<WorkflowAction> isFinished = workflowAction -> completeStatuses.contains(workflowAction.getStatus());
        Predicate<WorkflowAction> isRunning = workflowAction -> workflowAction.getStatus() == WorkflowAction.Status.RUNNING;

        // While workflow job is running
        while (super.getJobInfo(oozieWorkflowJobId).getStatus() == WorkflowJob.Status.RUNNING) {

            log.info("Workflow job '{}' is running. Status information(s) will be polled every {} second(s)", oozieWorkflowJobId, POLLING_SECONDS);
            List<WorkflowAction> workflowActions = super.getJobInfo(oozieWorkflowJobId).getActions();
            List<WorkflowAction> completedActions = workflowActions.stream()
                    .filter(isFinished)
                    .collect(Collectors.toList());

            // Log completed workflow actions (if any)
            if (!completedActions.isEmpty()) {
                log.info("Completed {} on workflow job '{}': [{}]",
                        WorkflowAction.class.getSimpleName(),
                        oozieWorkflowJobId,
                        completedActions.stream()
                                .sorted(Comparator.comparing(WorkflowAction::getStartTime))
                                .map(workflowAction -> String.format("'%s'", workflowAction.getName()))
                                .collect(Collectors.joining(", ")));
            }

            // Log running workflow actions (if any)
            List<WorkflowAction> runningActionList = workflowActions.stream()
                    .filter(isRunning)
                    .sorted(Comparator.comparing(WorkflowAction::getStartTime))
                    .collect(Collectors.toList());

            if (runningActionList.isEmpty()) {
                log.warn("Unable to detect any {} with status {}",
                        WorkflowAction.class.getSimpleName(),
                        WorkflowAction.Status.RUNNING.toString());
            } else {
                log.info("Report for {} '{}'",
                        WorkflowAction.class.getSimpleName(),
                        objectMapper.writeValueAsString(runningActionList.get(0)));
            }

            Thread.sleep(POLLING_SECONDS * 1000);
        }

        // Final workflow job report
        WorkflowJob workflowJob =  super.getJobInfo(oozieWorkflowJobId);
        log.info("Final report for workflow job '{}':\n{}\n", oozieWorkflowJobId, objectMapper.writeValueAsString(workflowJob));

        if (workflowJob.getStatus() == WorkflowJob.Status.SUCCEEDED) {
            log.info("Successfully executed workflow job '{}'", oozieWorkflowJobId);
        } else {
            log.warn("Unable to fully execute workflow job '{}'", oozieWorkflowJobId);
        }
    }
}
