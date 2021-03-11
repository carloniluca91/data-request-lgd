package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.record.OozieActionRecord;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.locator.UseClasspathSqlLocator;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

@UseClasspathSqlLocator
@RegisterBeanMapper(OozieActionRecord.class)
public interface OozieActionDao extends Dao<OozieActionRecord, String> {

    @Override
    @SqlQuery
    Optional<OozieActionRecord> findById(@Bind("id") String key);

    @SqlQuery
    List<OozieActionRecord> findByLauncherId(@Bind("id") String jobLauncherId);

    @Override
    @SqlUpdate
    OozieActionRecord save(@BindBean OozieActionRecord bean);

    @SqlBatch("save")
    List<OozieActionRecord> saveBatch(List<OozieActionRecord> oozieActionRecords);

}
