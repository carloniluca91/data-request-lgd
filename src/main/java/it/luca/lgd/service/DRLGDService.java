package it.luca.lgd.service;

import it.luca.lgd.jdbc.dao.WorkflowJobDao;
import it.luca.lgd.model.jdbc.OozieJobRecord;
import it.luca.lgd.oozie.WorkflowJobId;
import it.luca.lgd.oozie.WorkflowJobParameter;
import it.luca.lgd.utils.JobConfiguration;
import it.luca.lgd.utils.JobProperties;
import it.luca.lgd.utils.Tuple2;
import lombok.extern.slf4j.Slf4j;
import org.apache.oozie.client.OozieClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
public class DRLGDService {

    @Value("${oozie.server.url}")
    private String oozieServerUrl;

    @Autowired
    private WorkflowJobDao workflowJobDao;

    private OozieClient startOozieClient() {

        // Start OozieClient
        OozieClient oozieClient = new OozieClient(oozieServerUrl);
        log.info("Successfully connected to Oozie Server Url {}", oozieServerUrl);
        return oozieClient;
    }

    public Tuple2<Boolean, String> runWorkflowJob(WorkflowJobId workflowJobId, Map<WorkflowJobParameter, String> parameterStringMap) {

        try {
            WorkflowJobParameter oozieWfPath, pigScriptPath;
            switch (workflowJobId) {
                case CICLILAV_STEP1:
                    oozieWfPath = WorkflowJobParameter.CICLILAV_STEP1_WORKFLOW;
                    pigScriptPath = WorkflowJobParameter.CICLILAV_STEP1_PIG;
                    break;

                default:
                    oozieWfPath = WorkflowJobParameter.FPASPERD_WORKFLOW;
                    pigScriptPath = WorkflowJobParameter.FPASPERD_PIG;
                    break;
            }

            // Initialize job properties
            JobConfiguration jobConfiguration = new JobConfiguration("job.properties");
            JobProperties jobProperties = JobProperties.copyOf(jobConfiguration);

            // ... and then set specific workflow job parameters/properties (workflow job name, workflow job HDFS path, pig script HDFS path)
            jobProperties.setParameters(parameterStringMap);
            jobProperties.setParameter(WorkflowJobParameter.WORKFLOW_NAME, String.format("DataRequestLGD - %s", workflowJobId.getId()));
            jobProperties.setParameter(WorkflowJobParameter.WORKFLOW_PATH, jobConfiguration.getParameter(oozieWfPath));
            jobProperties.setParameter(WorkflowJobParameter.PIG_SCRIPT_PATH, jobConfiguration.getParameter(pigScriptPath));
            log.info("Provided properties for workflow job '{}': {}", workflowJobId.getId(), jobProperties.getPropertiesReport());

            String oozieWorkflowJobId = startOozieClient().run(jobProperties);
            log.info("Workflow job '{}' submitted ({})", workflowJobId.getId(), oozieWorkflowJobId);
            return new Tuple2<>(true, oozieWorkflowJobId);

        } catch (Exception e) {
            log.error("Unable to run workflow job '{}'. Stack trace: ", workflowJobId.getId(), e);
            return new Tuple2<>(false, e.getMessage());
        }
    }

    public OozieJobRecord monitorWorkflowJobExecution(String workflowJobId) {

        try {
            OozieClient oozieClient = startOozieClient();
            Optional<OozieJobRecord> oozieJobRecordOptional = workflowJobDao.findById(workflowJobId);
            if (oozieJobRecordOptional.isPresent()) {

                log.info("Workflow '{}' already defined", workflowJobId);
                return oozieJobRecordOptional.get();
            } else {
                log.warn("Workflow '{}' does not exist yet", workflowJobId);
                OozieJobRecord oozieJobRecord = OozieJobRecord.fromWorkflowJob(oozieClient.getJobInfo(workflowJobId));
                workflowJobDao.save(oozieJobRecord);
                return oozieJobRecord;
            }

        } catch (Exception e) {

            log.warn("Caught an exception while trying to poll information about Oozie job '{}'. Stack trace: ", workflowJobId, e);
            return null;
        }
    }
}
