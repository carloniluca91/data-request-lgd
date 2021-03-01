package it.luca.lgd.service;

import it.luca.lgd.jdbc.dao.OozieJobDao;
import it.luca.lgd.jdbc.model.OozieJobRecord;
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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DRLGDService {

    @Value("${oozie.server.url}")
    private String oozieServerUrl;

    @Autowired
    private OozieJobDao oozieJobDao;

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

            OozieClient oozieClient = startOozieClient();
            String oozieWorkflowJobId = oozieClient.run(jobProperties);
            log.info("Workflow job '{}' submitted ({})", workflowJobId.getId(), oozieWorkflowJobId);
            oozieJobDao.save(OozieJobRecord.fromWorkflowJob(oozieClient.getJobInfo(oozieWorkflowJobId)));
            return new Tuple2<>(true, oozieWorkflowJobId);

        } catch (Exception e) {
            log.error("Unable to run workflow job '{}'. Stack trace: ", workflowJobId.getId(), e);
            return new Tuple2<>(false, e.getMessage());
        }
    }

    public OozieJobRecord getOozieJobStatusById(String workflowJobId) {

        try {
            Optional<OozieJobRecord> oozieJobRecordOptional = oozieJobDao.findById(workflowJobId);
            if (oozieJobRecordOptional.isPresent()) {
                return oozieJobRecordOptional.get();
            } else {
                log.warn("Workflow job '{}' not found in table '{}'. Requesting information through {} API",
                        workflowJobId, oozieJobDao.fQTableName(), OozieClient.class.getName());
                OozieJobRecord oozieJobRecord = OozieJobRecord.fromWorkflowJob(startOozieClient().getJobInfo(workflowJobId));
                oozieJobDao.save(oozieJobRecord);
                return oozieJobRecord;
            }
        } catch (Exception e) {

            log.warn("Caught an exception while trying to poll information about Oozie job '{}'. Stack trace: ", workflowJobId, e);
            return null;
        }
    }

    public List<OozieJobRecord> getLastNOozieJobsStatuses(int n) {

        return oozieJobDao.lastNOozieJobs(n)
                .stream().sorted(Comparator.comparing(OozieJobRecord::getJobStartTime).reversed())
                .collect(Collectors.toList());
    }
}
