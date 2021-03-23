package it.luca.lgd.service;

import it.luca.lgd.exception.IllegalWorkflowIdException;
import it.luca.lgd.jdbc.dao.impl.DRLGDDao;
import it.luca.lgd.jdbc.record.OozieActionRecord;
import it.luca.lgd.jdbc.record.OozieJobRecord;
import it.luca.lgd.jdbc.record.RequestRecord;
import it.luca.lgd.model.JobParameters;
import it.luca.lgd.oozie.WorkflowJobLabel;
import it.luca.lgd.oozie.WorkflowJobParameter;
import it.luca.lgd.utils.JobConfiguration;
import it.luca.lgd.utils.JobProperties;
import it.luca.lgd.utils.Tuple2;
import lombok.extern.slf4j.Slf4j;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
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

    /**
     * Gets OozieClient instance
     * @return OozieClient
     */

    private OozieClient getOozieClientInstance() {

        OozieClient oozieClient = new OozieClient(oozieUrl);
        log.info("Successfully connected to Oozie Server Url {}", oozieUrl);
        return oozieClient;
    }

    /**
     * Retrieves WorkflowJob for given Oozie job id job id by means of OozieClient API
     * @param workflowJobId: workflow job id
     * @return WorkflowJob
     * @throws OozieClientException if given Oozie job id cannot be found
     */

    private WorkflowJob getWorkflowJob(String workflowJobId) throws OozieClientException {

        String className = OozieJobRecord.class.getSimpleName();
        String oozieClientName = OozieClient.class.getSimpleName();
        log.info("Retrieving {} for workflow job {} by means of {} API", className, workflowJobId, oozieClientName);
        WorkflowJob workflowJob = getOozieClientInstance().getJobInfo(workflowJobId);
        log.info("Retrieved {} for workflob job {} by means of {} API", className, workflowJobId, oozieClientName);
        return workflowJob;
    }

    /**
     * Submits Oozie job using given parameter map
     * @param workflowJobLabel: label of Oozie job to be submitted
     * @param parameters: parameters for current Oozie job
     * @return (true, generated Oozie job id) if job submission succeeded, (false, exception.getMessage()) if it failed
     */

    private Tuple2<Boolean, String> runWorkflowJob(WorkflowJobLabel workflowJobLabel, Map<WorkflowJobParameter, String> parameters) {

        try {
            // Initialize job properties and then set specific workflow job parameters/properties
            JobConfiguration jobConfiguration = new JobConfiguration("job.properties");
            JobProperties jobProperties = JobProperties.copyOf(jobConfiguration);
            jobProperties.setParameters(parameters);
            jobProperties.setParameter(WorkflowJobParameter.WORKFLOW_NAME, String.format("DataRequestLGD - %s", workflowJobLabel.getId()));
            WorkflowJobParameter oozieWfPath;
            switch (workflowJobLabel) {
                case FLIGHT_DETAILS:
                    oozieWfPath = WorkflowJobParameter.FLIGHT_DETAILS_APP_PATH;
                    break;
                case CANCELLED_FLIGHTS:
                    oozieWfPath = WorkflowJobParameter.CANCELLED_FLIGHTS_APP_PATH;
                    break;
                case MONTHLY_GROUPED_DELAYS:
                    oozieWfPath = WorkflowJobParameter.MONTHLY_GROUPED_DELAYS_APP_PATH;
                    break;
                default: throw new IllegalWorkflowIdException(workflowJobLabel);
            }

            jobProperties.setParameter(WorkflowJobParameter.WORKFLOW_PATH, jobConfiguration.getParameter(oozieWfPath));
            log.info("Properties for Oozie job {}: {}", workflowJobLabel.getId(), jobProperties.getPropertiesReport());
            String oozieWorkflowJobId = getOozieClientInstance().run(jobProperties);
            log.info("Workflow job {} submitted ({})", workflowJobLabel.getId(), oozieWorkflowJobId);
            return new Tuple2<>(true, oozieWorkflowJobId);
        } catch (Exception e) {
            log.error("Unable to run Oozie job {}. Stack trace: ", workflowJobLabel.getId(), e);
            return new Tuple2<>(false, e.getMessage());
        }
    }

    /**
     * Submits Oozie job using given parameters
     * @param workflowJobLabel: label of Oozie job to be submitted
     * @param jobParameters: parameters for current Oozie job
     * @param <T>: job parameters type (must extend class JobParameters)
     * @return RequestRecord reporting job submission outcome
     */

    public <T extends JobParameters> RequestRecord runOozieJob(WorkflowJobLabel workflowJobLabel, T jobParameters) {

        Tuple2<Boolean, String> inputValidation = jobParameters.validate();
        RequestRecord requestRecord;
        if (inputValidation.getT1()) {

            // If given input matches given criterium, run workflow job
            log.info("Successsully validated input for Oozie job {}. Parameters: {}", workflowJobLabel.getId(), jobParameters.asString());
            Tuple2<Boolean, String> jobSubmissionOutcome = runWorkflowJob(workflowJobLabel, jobParameters.toMap());
            requestRecord = RequestRecord.from(workflowJobLabel, jobParameters, jobSubmissionOutcome);
        } else {

            // Otherwise, report the issue (wrapped around inputValidation object)
            String errorMsg = String.format("Invalid input for Oozie job %s. Rationale: %s", workflowJobLabel.getId(), inputValidation.getT2());
            log.warn(errorMsg);
            requestRecord = RequestRecord.from(workflowJobLabel, jobParameters, inputValidation);
        }

        return saveRequestRecord(requestRecord);
    }

    /**
     * Retrieves OozieJobRecord for given Oozie job id
     * @param workflowJobId: workflow job id
     * @return OozieJobRecord if given id can be found by means of application DB or OozieClient API
     */

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
                    drlgdDao.saveOozieJob(oozieJobRecord);
                }
                return oozieJobRecord;
            }
        } catch (Exception e) {
            log.error("Exception while trying to poll information about Oozie job {}. Stack trace: ", workflowJobId, e);
            return null;
        }
    }

    /**
     * Returns list of OozieActionRecord for Oozie job with given id
     * @param workflowJobId: Oozie job id
     * @return list of OozieActionRecord for Oozie job with given id
     */

    public List<OozieActionRecord> findActionsForJob(String workflowJobId) {

        String className = OozieActionRecord.class.getSimpleName();
        try {

            // Check if some Oozie Actions can be retrieved from table
            List<OozieActionRecord> oozieActionsFromDb = drlgdDao.findOozieJobActions(workflowJobId);
            if (oozieActionsFromDb.isEmpty()) {

                // If not, retrieve Oozie Actions by means of Oozie Client API
                log.warn("Unable to find any {} for workflow job {} within application DB", className, workflowJobId);
                WorkflowJob workflowJob = getWorkflowJob(workflowJobId);
                List<OozieActionRecord> oozieActionsFromWorkflowJob = OozieActionRecord.batchFrom(workflowJob);
                if (COMPLETED.contains(workflowJob.getStatus())) {

                    // If workflowJob has completed, sssert that it has been inserted
                    Optional<OozieJobRecord> optionalOozieJobRecord = drlgdDao.findOozieJob(workflowJobId);
                    if (!optionalOozieJobRecord.isPresent()) {

                        log.warn("Unable to find any {} for workflow job {}", OozieJobRecord.class.getSimpleName(), workflowJob);
                        drlgdDao.saveOozieJob(OozieJobRecord.from(workflowJob));
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

    /***
     * Saves given RequestRecord into application DB and get back same object with generated key
     * @param requestRecord: RequestRecord to be saved into application DB
     * @return given RequestRecord with generated id
     */

    public RequestRecord saveRequestRecord(RequestRecord requestRecord) {

        return drlgdDao.saveRequest(requestRecord);
    }
}
