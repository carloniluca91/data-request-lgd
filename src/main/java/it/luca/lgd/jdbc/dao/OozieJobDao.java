package it.luca.lgd.jdbc.dao;

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
public interface OozieJobDao extends Dao<OozieJobRecord, String> {

    @Override
    @SqlQuery
    Optional<OozieJobRecord> findById(@Bind("id") String key);

    @Override
    @SqlUpdate
    OozieJobRecord save(@BindBean OozieJobRecord bean);

}
