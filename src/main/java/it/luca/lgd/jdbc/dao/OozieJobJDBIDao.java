package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.record.OozieJobRecord;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public interface OozieJobJDBIDao extends JDBIDao<OozieJobRecord, String> {

    @Override
    @SqlQuery
    @RegisterRowMapper()
    OozieJobRecord findById(@Bind("id") String key);

    @Override
    @SqlUpdate
    void save(OozieJobRecord bean);
}
