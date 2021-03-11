package it.luca.lgd.service;

import it.luca.lgd.exception.IllegalWorkflowIdException;
import it.luca.lgd.jdbc.dao.DRLGDDao;
import it.luca.lgd.jdbc.record.OozieActionRecord;
import it.luca.lgd.jdbc.record.OozieJobRecord;
import it.luca.lgd.jdbc.record.RequestRecord;
import it.luca.lgd.oozie.WorkflowJobId;
import it.luca.lgd.oozie.WorkflowJobParameter;
import it.luca.lgd.oozie.WorkflowJobStatuses;
import it.luca.lgd.utils.JobConfiguration;
import it.luca.lgd.utils.JobProperties;
import it.luca.lgd.utils.Tuple2;
import lombok.extern.slf4j.Slf4j;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
public class DRLGDService {

    @Value("${oozie.url}")
    private String oozieUrl;

    @Autowired
    private DRLGDDao drlgdDao;

    private OozieClient startOozieClient() {

        OozieClient oozieClient = new OozieClient(oozieUrl);
        log.info("Successfully connected to Oozie Server Url {}", oozieUrl);
        return oozieClient;
    }

    private WorkflowJob getWorkflowJob(String workflowJobId) throws Exception {

        // If job has completed, insert records on both Oozie Job and Oozie Action table
        log.info("Retrieving report about workflow job {} by means of {} API", workflowJobId, OozieClient.class.getName());
        WorkflowJob workflowJob = startOozieClient().getJobInfo(workflowJobId);
        log.info("Retrieved report about workflob job {}", workflowJobId);
        return workflowJob;
    }

    public Tuple2<Boolean, String> runWorkflowJob(WorkflowJobId workflowJobId, Map<WorkflowJobParameter, String> parameterStringMap) {

        try {

            WorkflowJobParameter oozieWfPath;
            switch (workflowJobId) {
                case CANCELLED_FLIGHTS:
                    oozieWfPath = WorkflowJobParameter.CANCELLED_FLIGHTS_APP_PATH;
                    break;
                case FPASPERD:
                    oozieWfPath = WorkflowJobParameter.FPASPERD_WORKFLOW;
                    break;
                default:
                    throw new IllegalWorkflowIdException(workflowJobId);
            }

            // Initialize job properties
            JobConfiguration jobConfiguration = new JobConfiguration("job.properties");
            JobProperties jobProperties = JobProperties.copyOf(jobConfiguration);

            // ... and then set specific workflow job parameters/properties (workflow job name, workflow job HDFS path, pig script HDFS path)
            jobProperties.setParameters(parameterStringMap);
            jobProperties.setParameter(WorkflowJobParameter.WORKFLOW_NAME, String.format("DataRequestLGD - %s", workflowJobId.getId()));
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

    public OozieJobRecord findOozieJob(String workflowJobId) {

        String className = OozieJobRecord.class.getName();
        try {

            // Check if the Oozie job has already been inserted into Oozie job table
            Optional<OozieJobRecord> optionalOozieJobRecord = drlgdDao.findOozieJob(workflowJobId);
            if (optionalOozieJobRecord.isPresent()) {
                log.info("Retrieved {} with id {} within application DB", className, workflowJobId);
                return optionalOozieJobRecord.get();
            } else {
                // If not, check its status. Insert into table if terminated
                WorkflowJob workflowJob = getWorkflowJob(workflowJobId);
                OozieJobRecord oozieJobRecord = OozieJobRecord.from(workflowJob);
                log.info("Retrieved {} with id {} from {} API", className, workflowJobId, OozieClient.class.getSimpleName());
                return WorkflowJobStatuses.COMPLETED.contains(workflowJob.getStatus()) ?
                        drlgdDao.saveOozieJobRecord(oozieJobRecord) :
                        oozieJobRecord;
            }
        } catch (Exception e) {
            log.error("Exception while trying to poll information about Oozie job {}. Stack trace: ", workflowJobId, e);
            return null;
        }
    }

    public List<OozieActionRecord> findOozieJobActions(String workflowJobId) {

        String className = OozieActionRecord.class.getName();
        try {

            // Check if some Oozie Actions can be retrieved from table
            List<OozieActionRecord> oozieActionsFromDb = drlgdDao.findOozieJobActions(workflowJobId);
            if (oozieActionsFromDb.isEmpty()) {

                // If not, retrieve Oozie Actions by means of Oozie Client API
                log.warn("Unable to find any {} for workflowJob {} within application DB", className, workflowJobId);
                WorkflowJob workflowJob = getWorkflowJob(workflowJobId);
                List<OozieActionRecord> oozieActionsFromWorkflowJob = OozieActionRecord.batchFrom(workflowJob);

                // If workflowJob has completed
                if (WorkflowJobStatuses.COMPLETED.contains(workflowJob.getStatus())) {

                    // Assert that it has been inserted
                    Optional<OozieJobRecord> optionalOozieJobRecord = drlgdDao.findOozieJob(workflowJobId);
                    if (!optionalOozieJobRecord.isPresent()) {

                        log.warn("Unable to find any {} for workflowJob {}", OozieJobRecord.class.getSimpleName(), workflowJob);
                        drlgdDao.saveOozieJobRecord(OozieJobRecord.from(workflowJob));
                    }
                    // Save and return Oozie actions just saved
                    return drlgdDao.saveOozieActions(oozieActionsFromWorkflowJob);
                } else { return oozieActionsFromWorkflowJob; }
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
