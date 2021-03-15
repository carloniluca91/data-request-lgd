package it.luca.lgd.jdbc.dao.impl;

import it.luca.lgd.jdbc.dao.common.Find;
import it.luca.lgd.jdbc.dao.common.SaveBatch;
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
public interface OozieActionDao extends Find<OozieActionRecord, String>, SaveBatch<OozieActionRecord> {

    @Override
    @SqlQuery
    Optional<OozieActionRecord> findByKey(@Bind("id") String key);

    @SqlQuery
    List<OozieActionRecord> findByLauncherId(@Bind("id") String jobLauncherId);

    @Override
    @SqlBatch
    void save(@BindBean List<OozieActionRecord> oozieActionRecords);

}
