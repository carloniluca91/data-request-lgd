package it.luca.lgd.jdbc.dao.impl;

import it.luca.lgd.jdbc.dao.common.Find;
import it.luca.lgd.jdbc.dao.common.Save;
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
public interface OozieJobDao extends Find<OozieJobRecord, String>, Save<OozieJobRecord> {

    @Override
    @SqlQuery
    Optional<OozieJobRecord> findByKey(@Bind("id") String key);

    @Override
    @SqlUpdate
    void save(@BindBean OozieJobRecord bean);
}
