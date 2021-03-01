package it.luca.lgd.jdbc.mapper;

import it.luca.lgd.jdbc.table.TableDefinition;
import lombok.AllArgsConstructor;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;

@AllArgsConstructor
public abstract class DRLGDResultSetExtractor<T extends TableDefinition<R>, R> implements ResultSetExtractor<R> {

    protected final T tableDefinition;

    protected abstract R fromResultSet(ResultSet resultSet) throws SQLException;

    @Override
    public R extractData(ResultSet resultSet) throws SQLException, DataAccessException {

        return resultSet.next() ? fromResultSet(resultSet) : null;
    }
}
