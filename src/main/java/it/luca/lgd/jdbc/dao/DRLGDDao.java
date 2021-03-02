package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.record.DRLGDRecord;
import it.luca.lgd.jdbc.table.DRLGDTable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public abstract class DRLGDDao<R extends DRLGDRecord, T extends DRLGDTable<R>> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    protected final T tableDefinition;

    public DRLGDDao(T tableDefinition) {

        this.tableDefinition = tableDefinition;
    }

    public String fQTableName() {
        return tableDefinition.fQTableName();
    }

    public Optional<R> findById(Object... args) {

        String whereCondition = tableDefinition.primaryKeyColumns()
                .stream().map(s -> String.format("%s = ?", s))
                .collect(Collectors.joining(" AND "));

        String SELECT_BY_ID = String.format("SELECT * FROM %s WHERE %s", fQTableName(), whereCondition);
        return Optional.ofNullable(jdbcTemplate.query(SELECT_BY_ID, tableDefinition.getResultSetExtractor(), args));
    }

    public void save(R object) {

        String tClassName = tableDefinition.tClassName();
        String primaryKeyColumns = String.join("|", tableDefinition.primaryKeyColumns());
        String primaryKeyValues = Arrays.stream(object.primaryKeyValues())
                .map(Object::toString)
                .collect(Collectors.joining("|"));
        String allColumns = String.join(", ", tableDefinition.allColumns());
        String nQuestionMarks = String.join(", ", Collections.nCopies(this.tableDefinition.allColumns().size(), "?"));

        log.info("Saving {} object with primaryKey ({}) = ({})", tClassName, primaryKeyColumns, primaryKeyValues);
        String INSERT_INTO = String.format("INSERT INTO %s (%s) VALUES (%s)", fQTableName(), allColumns, nQuestionMarks);
        jdbcTemplate.update(INSERT_INTO, object.allValues());
        log.info("Successfully saved {} object into table {}", tClassName, fQTableName());
    }

    public void saveBatch(List<R> rList) {

        String tClassName = tableDefinition.tClassName();
        String primaryKeyColumns = String.join("|", tableDefinition.primaryKeyColumns());
        String primaryKeyValueSet = rList.stream().map(r -> {
                    String primaryKeyValues = Stream.of(r.primaryKeyValues()).map(Object::toString).collect(Collectors.joining("|"));
                    return String.format("(%s)", primaryKeyValues);
                }).collect(Collectors.joining(", "));

        String allColumns = String.join(", ", tableDefinition.allColumns());
        String nQuestionMarks = String.join(", ", Collections.nCopies(this.tableDefinition.allColumns().size(), "?"));
        log.info("Saving {} {} object(s) with following primaryKey set(s) ({}) in {}", rList.size(), tClassName, primaryKeyColumns, primaryKeyValueSet);
        String INSERT_BATCH_INTO = String.format("INSERT INTO %s (%s) VALUES (%s)", fQTableName(), allColumns, nQuestionMarks);
        jdbcTemplate.batchUpdate(INSERT_BATCH_INTO, rList.stream().map(DRLGDRecord::allValues).collect(Collectors.toList()));
        log.info("Successfully saved {} {} object(s) into table {}", rList.size(), tClassName, fQTableName());
    }
}
