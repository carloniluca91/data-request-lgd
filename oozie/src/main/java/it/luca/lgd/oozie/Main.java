package it.luca.lgd.oozie;

import it.luca.lgd.oozie.client.DRLGDOozieClient;
import it.luca.lgd.oozie.job.WorkflowJobId;
import it.luca.lgd.oozie.job.WorkflowJobParameter;

import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String... args) throws Exception {

        Map<WorkflowJobParameter, String> parameterMap = new HashMap<WorkflowJobParameter, String>(){{
            put(WorkflowJobParameter.START_DATE, "1991-01-01");
            put(WorkflowJobParameter.END_DATE, "1991-12-31");
        }};

        DRLGDOozieClient DRLGDOozieClient = new DRLGDOozieClient("http://quickstart.cloudera:11000/oozie/");
        DRLGDOozieClient.runWorkflowJob(WorkflowJobId.CICLILAV_STEP1, parameterMap);
    }
}
