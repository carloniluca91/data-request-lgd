package it.luca.lgd.oozie;

import it.luca.lgd.oozie.client.DataRequestLGDOozieClient;
import it.luca.lgd.oozie.job.JobId;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.oozie.client.OozieClientException;

public class Main {

    public static void main(String... args) throws InterruptedException, OozieClientException, ConfigurationException {

        DataRequestLGDOozieClient dataRequestLGDOozieClient = new DataRequestLGDOozieClient("http://quickstart.cloudera:11000/oozie/");
        dataRequestLGDOozieClient.runWorkflowJob(JobId.CICLILAV_STEP1);
    }
}
