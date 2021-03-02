package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.model.OozieActionRecord;
import it.luca.lgd.jdbc.table.OozieActionTable;

public abstract class OozieActionDao extends DRLGDDao<OozieActionRecord, OozieActionTable> {

    public OozieActionDao() {
        super(new OozieActionTable());
    }
}
