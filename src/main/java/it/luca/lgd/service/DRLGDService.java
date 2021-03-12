package it.luca.lgd.service;

import it.luca.lgd.exception.IllegalWorkflowIdException;
import it.luca.lgd.jdbc.dao.DRLGDDao;
import it.luca.lgd.jdbc.record.OozieActionRecord;
import it.luca.lgd.jdbc.record.OozieJobRecord;
import it.luca.lgd.jdbc.record.RequestRecord;
import it.luca.lgd.model.parameters.JobParameters;
import it.luca.lgd.oozie.WorkflowJobId;
import it.luca.lgd.oozie.WorkflowJobParameter;
import it.luca.lgd.utils.JobConfiguration;
import it.luca.lgd.utils.JobProperties;
import it.luca.lgd.utils.Tuple2;
import lombok.extern.slf4j.Slf4j;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

@Slf4j
@Service
public class DRLGDService {

    @Value("${oozie.url}")
    private String oozieUrl;

    @Autowired
    private DRLGDDao drlgdDao;

    private final List<WorkflowJob.Status> COMPLETED = Arrays.asList(WorkflowJob.Status.SUCCEEDED,
            WorkflowJob.Status.FAILED,
            WorkflowJob.Status.KILLED);

    private OozieClient startOozieClient() {

        OozieClient oozieClient = new OozieClient(oozieUrl);
        log.info("Successfully connected to Oozie Server Url {}", oozieUrl);
        return oozieClient;
    }

    private WorkflowJob getWorkflowJob(String workflowJobId) throws Exception {

        // If job has completed, insert records on both Oozie Job and Oozie Action table
        String className = OozieJobRecord.class.getSimpleName();
        String oozieClientName = OozieClient.class.getSimpleName();
        log.info("Retrieving {} for workflow job {} by means of {} API", className, workflowJobId, oozieClientName);
        WorkflowJob workflowJob = startOozieClient().getJobInfo(workflowJobId);
        log.info("Retrieved {} for workflob job {} by means of {} API", className, workflowJobId, oozieClientName);
        return workflowJob;
    }

    public <T extends JobParameters> RequestRecord runOozieJob(WorkflowJobId workflowJobId, T jobParameters) {

        Tuple2<Boolean, String> inputValidation = jobParameters.validate();
        RequestRecord requestRecord;
        if (inputValidation.getT1()) {

            // If provided input matches given criterium, run workflow job
            log.info("Successsully validated input for Oozie job {}. Parameters: {}", workflowJobId.getId(), jobParameters.asString());
            Tuple2<Boolean, String> jobSubmissionOutcome = runWorkflowJob(workflowJobId, jobParameters.toMap());
            requestRecord = RequestRecord.from(workflowJobId, jobParameters, jobSubmissionOutcome);
        } else {

            // Otherwise, report the issue (wrapped around inputValidation object)
            String errorMsg = String.format("Invalid input for Oozie job %s. Rationale: %s", workflowJobId.getId(), inputValidation.getT2());
            log.warn(errorMsg);
            requestRecord = RequestRecord.from(workflowJobId, jobParameters, inputValidation);
        }

        return saveRequestRecord(requestRecord);
    }

    private Tuple2<Boolean, String> runWorkflowJob(WorkflowJobId workflowJobId, Map<WorkflowJobParameter, String> parameterStringMap) {

        try {

            // Initialize job properties and then set specific workflow job parameters/properties
            JobConfiguration jobConfiguration = new JobConfiguration("job.properties");
            JobProperties jobProperties = JobProperties.copyOf(jobConfiguration);
            jobProperties.setParameters(parameterStringMap);
            jobProperties.setParameter(WorkflowJobParameter.WORKFLOW_NAME, String.format("DataRequestLGD - %s", workflowJobId.getId()));
            WorkflowJobParameter oozieWfPath;
            switch (workflowJobId) {
                case CANCELLED_FLIGHTS:
                    oozieWfPath = WorkflowJobParameter.CANCELLED_FLIGHTS_APP_PATH;
                    break;
                case FPASPERD:
                    oozieWfPath = WorkflowJobParameter.FPASPERD_WORKFLOW;
                    break;
                default: throw new IllegalWorkflowIdException(workflowJobId);
            }

            jobProperties.setParameter(WorkflowJobParameter.WORKFLOW_PATH, jobConfiguration.getParameter(oozieWfPath));
            log.info("Provided properties for Oozie job {}: {}", workflowJobId.getId(), jobProperties.getPropertiesReport());
            String oozieWorkflowJobId = startOozieClient().run(jobProperties);
            log.info("Workflow job {} submitted ({})", workflowJobId.getId(), oozieWorkflowJobId);
            return new Tuple2<>(true, oozieWorkflowJobId);

        } catch (Exception e) {
            log.error("Unable to run Oozie job {}. Stack trace: ", workflowJobId.getId(), e);
            return new Tuple2<>(false, e.getMessage());
        }
    }

    public OozieJobRecord findJob(String workflowJobId) {

        String className = OozieJobRecord.class.getSimpleName();
        try {

            // Check if the Oozie job has already been inserted into Oozie job table
            Optional<OozieJobRecord> optionalOozieJobRecord = drlgdDao.findOozieJob(workflowJobId);
            if (optionalOozieJobRecord.isPresent()) {

                log.info("Retrieved {} with id {} within application DB", className, workflowJobId);
                return optionalOozieJobRecord.get();
            } else {

                // If not, check its status. Insert into table if terminated
                log.warn("Unable to retrieve any {} with id {} from application DB", className, workflowJobId);
                WorkflowJob workflowJob = getWorkflowJob(workflowJobId);
                OozieJobRecord oozieJobRecord = OozieJobRecord.from(workflowJob);
                if (COMPLETED.contains(workflowJob.getStatus())) {
                    drlgdDao.saveOozieJobRecord(oozieJobRecord);
                }
                return oozieJobRecord;
            }
        } catch (Exception e) {
            log.error("Exception while trying to poll information about Oozie job {}. Stack trace: ", workflowJobId, e);
            return null;
        }
    }

    public List<OozieActionRecord> findActionsForJob(String workflowJobId) {

        String className = OozieActionRecord.class.getSimpleName();
        try {

            // Check if some Oozie Actions can be retrieved from table
            List<OozieActionRecord> oozieActionsFromDb = drlgdDao.findOozieJobActions(workflowJobId);
            if (oozieActionsFromDb.isEmpty()) {

                // If not, retrieve Oozie Actions by means of Oozie Client API
                log.warn("Unable to find any {} for workflowJob {} within application DB", className, workflowJobId);
                WorkflowJob workflowJob = getWorkflowJob(workflowJobId);
                List<OozieActionRecord> oozieActionsFromWorkflowJob = OozieActionRecord.batchFrom(workflowJob);
                if (COMPLETED.contains(workflowJob.getStatus())) {

                    // If workflowJob has completed, sssert that it has been inserted
                    Optional<OozieJobRecord> optionalOozieJobRecord = drlgdDao.findOozieJob(workflowJobId);
                    if (!optionalOozieJobRecord.isPresent()) {

                        log.warn("Unable to find any {} for workflowJob {}", OozieJobRecord.class.getSimpleName(), workflowJob);
                        drlgdDao.saveOozieJobRecord(OozieJobRecord.from(workflowJob));
                    }

                    drlgdDao.saveOozieActions(oozieActionsFromWorkflowJob);
                }
                return oozieActionsFromWorkflowJob;
            } else { return oozieActionsFromDb; }
        } catch (Exception e) {
            log.error("Exception while trying to retrieve {}(s) for Oozie job {}. Stack trace: ", className, workflowJobId, e);
            return Collections.emptyList();
        }
    }

    public RequestRecord saveRequestRecord(RequestRecord requestRecord) {

        return drlgdDao.saveRequestRecord(requestRecord);
    }
}
