package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.table.OozieJobTableDefinition;
import it.luca.lgd.jdbc.model.OozieJobRecord;
import org.apache.oozie.client.WorkflowJob;

import java.util.List;
import java.util.Optional;

public abstract class OozieJobDao extends BaseDao<OozieJobTableDefinition, OozieJobRecord, String> {

    public OozieJobDao() {
        super(new OozieJobTableDefinition());
    }

    public abstract List<OozieJobRecord> lastNOozieJobs(int n);

    public abstract Optional<OozieJobRecord> lastOozieJobWithStatus(WorkflowJob.Status status);
}
