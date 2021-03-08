package it.luca.lgd.jdbc.table;

import it.luca.lgd.exception.IllegalTableEntityException;
import it.luca.lgd.jdbc.record.DRLGDRecord;
import lombok.AllArgsConstructor;
import org.springframework.jdbc.core.ResultSetExtractor;

import javax.persistence.Table;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

@AllArgsConstructor
public abstract class DRLGDTable<R extends DRLGDRecord> {

    protected final String TS_INSERT = "ts_insert";
    protected final String DT_INSERT = "dt_insert";

    private final Class<R> tClass;

    public abstract List<String> allColumns();

    public ResultSetExtractor<R> getResultSetExtractor() {

        return resultSet -> resultSet.next() ?
                fromResultSetToTableRecord(resultSet) : null;
    }

    protected abstract R fromResultSetToTableRecord(ResultSet rs) throws SQLException;

    public String fQTableName() {
        return schema() + "." + tableName();
    }

    public abstract List<String> primaryKeyColumns();

    public String schema() {

        return Optional.ofNullable(tClass.getAnnotation(Table.class))
                .map(Table::schema)
                .orElseThrow(() -> new IllegalTableEntityException(tClass, "schema"));
    }

    public String tableName() {

        return Optional.ofNullable(tClass.getAnnotation(Table.class))
                .map(Table::name)
                .orElseThrow(() -> new IllegalTableEntityException(tClass, "name"));
    }

    public String tClassName() {
        return tClass.getSimpleName();
    }
}
