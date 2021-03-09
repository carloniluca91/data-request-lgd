package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.record.DRLGDRecord;
import it.luca.lgd.jdbc.table.DRLGDTable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public abstract class DRLGDDao<R extends DRLGDRecord, T extends DRLGDTable<R>> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    protected final T table;

    public DRLGDDao(T table) {

        this.table = table;
    }

    public String tClassName() {
        return table.tClassName();
    }

    public String fQTableName() {
        return table.fQTableName();
    }

    public Optional<R> findById(Object... args) {

        String whereCondition = table.primaryKeyColumns().stream()
                .map(s -> String.format("%s = ?", s))
                .collect(Collectors.joining(" AND "));

        String tClassName = table.tClassName();
        String SELECT_BY_ID = String.format("SELECT * FROM %s WHERE %s", fQTableName(), whereCondition);
        log.info("Retrieving 1 {} object from table '{}'", tClassName, fQTableName());
        Optional<R> optionalR = Optional.ofNullable(jdbcTemplate.query(SELECT_BY_ID, table.getResultSetExtractor(), args));
        if (optionalR.isPresent()) {
            log.info("Retrieved 1 {} object from table '{}'", tClassName, fQTableName());
        } else {
            log.warn("Unable to retrieve any {} object from table '{}'", tClassName, fQTableName());
        }

        return optionalR;
    }

    public int saveAndGetKey(R object) {

        SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(jdbcTemplate);
        simpleJdbcInsert.withTableName(table.fQTableName())
                .usingGeneratedKeyColumns(table.primaryKeyColumns().toArray(new String[]{}));
        return simpleJdbcInsert
                .executeAndReturnKey(table.fromRecordToMapSqlParameterSource(object))
                .intValue();
    }

    public void save(R object) {

        String tClassName = table.tClassName();
        String allColumns = String.join(", ", table.allColumns());
        String nQuestionMarks = String.join(", ", Collections.nCopies(this.table.allColumns().size(), "?"));

        log.info("Saving {} object into table '{}'", tClassName, fQTableName());
        String INSERT_INTO = String.format("INSERT INTO %s (%s) VALUES (%s)", fQTableName(), allColumns, nQuestionMarks);
        jdbcTemplate.update(INSERT_INTO, object.allValues());
        log.info("Saved {} object into table '{}'", tClassName, fQTableName());
    }

    public void saveBatch(List<R> rList) {

        String tClassName = table.tClassName();
        String primaryKeyColumns = String.join("|", table.primaryKeyColumns());
        String primaryKeyValueSet = rList.stream()
                .map(r -> {
                    String primaryKeyValues = Stream.of(r.primaryKeyValues())
                            .map(Object::toString)
                            .collect(Collectors.joining("|"));
                    return String.format("(%s)", primaryKeyValues);
                }).collect(Collectors.joining(", "));

        String allColumns = String.join(", ", table.allColumns());
        String nQuestionMarks = String.join(", ", Collections.nCopies(this.table.allColumns().size(), "?"));
        log.info("Saving {} {} object(s) with following primaryKey set(s) ({}) in {}", rList.size(), tClassName, primaryKeyColumns, primaryKeyValueSet);
        String INSERT_BATCH_INTO = String.format("INSERT INTO %s (%s) VALUES (%s)", fQTableName(), allColumns, nQuestionMarks);
        jdbcTemplate.batchUpdate(INSERT_BATCH_INTO, rList.stream().map(DRLGDRecord::allValues).collect(Collectors.toList()));
        log.info("Saved {} {} object(s) into table '{}'", rList.size(), tClassName, fQTableName());
    }
}
