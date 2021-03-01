package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.table.OozieJobTableDefinition;
import it.luca.lgd.model.jdbc.OozieJobRecord;
import org.apache.oozie.client.WorkflowJob;

import java.util.Optional;

public abstract class WorkflowJobDao extends BaseDao<OozieJobRecord, String, OozieJobTableDefinition> {

    public WorkflowJobDao() {
        super(new OozieJobTableDefinition());
    }

    public abstract Optional<OozieJobRecord> lastOozieJob();

    public abstract Optional<OozieJobRecord> lastOozieJobWithStatus(WorkflowJob.Status status);
}
