package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.common.FindDao;
import it.luca.lgd.jdbc.common.SaveBatchDao;
import it.luca.lgd.jdbc.record.OozieActionRecord;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.locator.UseClasspathSqlLocator;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

import java.util.List;
import java.util.Optional;

@UseClasspathSqlLocator
@RegisterBeanMapper(OozieActionRecord.class)
public interface OozieActionDao extends FindDao<OozieActionRecord, String>, SaveBatchDao<OozieActionRecord> {

    @Override
    @SqlQuery
    Optional<OozieActionRecord> findById(@Bind("id") String key);

    @SqlQuery
    List<OozieActionRecord> findByLauncherId(@Bind("id") String jobLauncherId);

    @Override
    @SqlBatch
    void save(@BindBean List<OozieActionRecord> oozieActionRecords);

}
