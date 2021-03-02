package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.record.OozieActionRecord;
import it.luca.lgd.jdbc.table.OozieActionTable;

import java.util.List;

public abstract class OozieActionDao extends DRLGDDao<OozieActionRecord, OozieActionTable> {

    public OozieActionDao() {
        super(new OozieActionTable());
    }

    public abstract List<OozieActionRecord> getOozieJobActions(String workflowJobId);
}
