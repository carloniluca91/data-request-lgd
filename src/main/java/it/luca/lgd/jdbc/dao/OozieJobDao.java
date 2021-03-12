package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.common.FindDao;
import it.luca.lgd.jdbc.common.SaveDao;
import it.luca.lgd.jdbc.record.OozieJobRecord;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.locator.UseClasspathSqlLocator;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.Optional;

@UseClasspathSqlLocator
@RegisterBeanMapper(OozieJobRecord.class)
public interface OozieJobDao extends FindDao<OozieJobRecord, String>, SaveDao<OozieJobRecord> {

    @Override
    @SqlQuery
    Optional<OozieJobRecord> findById(@Bind("id") String key);

    @Override
    @SqlUpdate
    void save(@BindBean OozieJobRecord bean);
}
