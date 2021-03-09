package it.luca.lgd.service;

import it.luca.lgd.jdbc.dao.OozieActionDao;
import it.luca.lgd.jdbc.dao.OozieJobDao;
import it.luca.lgd.jdbc.dao.RequestDao;
import it.luca.lgd.jdbc.record.OozieActionRecord;
import it.luca.lgd.jdbc.record.OozieJobRecord;
import it.luca.lgd.jdbc.record.RequestRecord;
import it.luca.lgd.oozie.OozieJobStatuses;
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
    private OozieJobDao oozieJobDao;

    @Autowired
    private OozieActionDao oozieActionDao;

    @Autowired
    private RequestDao requestDao;

    private OozieClient startOozieClient() {

        OozieClient oozieClient = new OozieClient(oozieUrl);
        log.info("Successfully connected to Oozie Server Url {}", oozieUrl);
        return oozieClient;
    }

    private WorkflowJob getWorkflowJob(String workflowJobId) throws Exception {

        // If job has completed, insert records on both Oozie Job and Oozie Action table
        log.info("Retrieving information about workflow job '{}' by means of {} API", workflowJobId, OozieClient.class.getName());
        WorkflowJob workflowJob = startOozieClient().getJobInfo(workflowJobId);
        WorkflowJob.Status status = workflowJob.getStatus();
        boolean hasCompleted = OozieJobStatuses.COMPLETED.contains(status);
        if (hasCompleted) {

            oozieJobDao.save(OozieJobRecord.from(workflowJob));
            oozieActionDao.saveBatch(OozieActionRecord.batchFrom(workflowJob));
        }

        log.info("Workflow job '{}' has {} completed (status {})", workflowJobId, hasCompleted ? "" : "not", status);
        return workflowJob;
    }

    /*
     *************************
     * OOZIE JOBS SUBMISSION *
     *************************
     */

    public RequestRecord getRequestRecordById(int id) {

        return requestDao.findById(id).orElse(null);
    }

    public int insertRequestRecord(RequestRecord requestRecord) {

        return requestDao.saveAndGetKey(requestRecord);
    }

    public Tuple2<Boolean, String> runWorkflowJob(WorkflowJobId workflowJobId, Map<WorkflowJobParameter, String> parameterStringMap) {

        try {

            WorkflowJobParameter oozieWfPath;
            switch (workflowJobId) {
                case CANCELLED_FLIGHTS:
                    oozieWfPath = WorkflowJobParameter.CANCELLED_FLIGHTS_APP_PATH;
                    break;

                default:
                    oozieWfPath = WorkflowJobParameter.FPASPERD_WORKFLOW;
                    break;
            }

            // Initialize job properties
            JobConfiguration jobConfiguration = new JobConfiguration("job.properties");
            JobProperties jobProperties = JobProperties.copyOf(jobConfiguration);

            // ... and then set specific workflow job parameters/properties (workflow job name, workflow job HDFS path, pig script HDFS path)
            jobProperties.setParameters(parameterStringMap);
            jobProperties.setParameter(WorkflowJobParameter.WORKFLOW_NAME, String.format("DataRequestLGD - %s", workflowJobId.getId()));
            jobProperties.setParameter(WorkflowJobParameter.WORKFLOW_PATH, jobConfiguration.getParameter(oozieWfPath));
            log.info("Provided properties for workflow job '{}': {}", workflowJobId.getId(), jobProperties.getPropertiesReport());

            String oozieWorkflowJobId = startOozieClient().run(jobProperties);
            log.info("Workflow job '{}' submitted ({})", workflowJobId.getId(), oozieWorkflowJobId);
            return new Tuple2<>(true, oozieWorkflowJobId);

        } catch (Exception e) {
            log.error("Unable to run workflow job '{}'. Stack trace: ", workflowJobId.getId(), e);
            return new Tuple2<>(false, e.getMessage());
        }
    }

    /*
     *************************
     * OOZIE JOBS MONITORING *
     *************************
     */

    public OozieJobRecord getOozieJobStatus(String workflowJobId) {

        try {
            // Check if this Oozie job has been inserted into Oozie job table
            Optional<OozieJobRecord> optionalOozieJobRecord = oozieJobDao.findById(workflowJobId);
            if (optionalOozieJobRecord.isPresent()) {
                return optionalOozieJobRecord.get();
            } else {
                WorkflowJob workflowJob = getWorkflowJob(workflowJobId);
                return OozieJobRecord.from(workflowJob);
            }
        } catch (Exception e) {
            log.error("Exception while trying to poll information about Oozie job '{}'. Stack trace: ", workflowJobId, e);
            return null;
        }
    }

    public List<OozieActionRecord> getOozieJobActions(String workflowJobId) {

        String tClassName = oozieActionDao.tClassName();
        try {

            // Check if some Oozie Actions can be retrieved for provided Oozie Job id
            List<OozieActionRecord> oozieActionRecords = oozieActionDao.getOozieJobActions(workflowJobId);
            return oozieActionRecords.isEmpty() ?
                    OozieActionRecord.batchFrom(getWorkflowJob(workflowJobId)) :
                    oozieActionRecords;

        } catch (Exception e) {
            log.error("Exception while trying to retrieve {}(s) of Oozie job '{}'. Stack trace: ", tClassName, workflowJobId, e);
            return Collections.emptyList();
        }
    }
}
