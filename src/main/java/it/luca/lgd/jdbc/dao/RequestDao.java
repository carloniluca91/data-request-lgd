package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.record.RequestRecord;
import it.luca.lgd.jdbc.table.RequestTable;

public abstract class RequestDao extends DRLGDDao<RequestRecord, RequestTable> {

    public RequestDao() {
        super(new RequestTable());
    }
}
