package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.binding.RequestBinding;
import it.luca.lgd.jdbc.mapper.RequestMapper;
import it.luca.lgd.jdbc.record.RequestRecord;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.locator.UseClasspathSqlLocator;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.Optional;

@UseClasspathSqlLocator
@RegisterRowMapper(RequestMapper.class)
public interface RequestDao extends Dao<RequestRecord, Integer> {

    @Override
    @SqlQuery
    Optional<RequestRecord> findById(@Bind("id") Integer key);

    @Override
    @SqlUpdate
    @GetGeneratedKeys
    RequestRecord save(@RequestBinding RequestRecord bean);
}
