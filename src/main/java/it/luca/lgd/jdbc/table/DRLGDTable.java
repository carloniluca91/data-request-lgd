package it.luca.lgd.jdbc.table;

import it.luca.lgd.jdbc.record.DRLGDRecord;
import lombok.AllArgsConstructor;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

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

    public Map<String, Object> fromRecordToMapSqlParameterSource(R record) {

        throw new UnsupportedOperationException();
    }

    protected abstract R fromResultSetToTableRecord(ResultSet rs) throws SQLException;

    public String fQTableName() {
        return schema() + "." + tableName();
    }

    public abstract List<String> primaryKeyColumns();

    public String schema() {

        return "oozie";
    }

    public abstract String tableName();

    public String tClassName() {
        return tClass.getSimpleName();
    }
}
