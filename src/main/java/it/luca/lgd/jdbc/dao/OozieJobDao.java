package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.record.OozieJobRecord;
import it.luca.lgd.jdbc.table.OozieJobTable;
import org.apache.oozie.client.WorkflowJob;

import java.util.List;
import java.util.Optional;

public abstract class OozieJobDao extends DRLGDDao<OozieJobRecord, OozieJobTable> {

    public OozieJobDao() {
        super(new OozieJobTable());
    }
}
