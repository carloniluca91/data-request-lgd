package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.record.DRLGDRecord;
import it.luca.lgd.jdbc.table.DRLGDTable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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

    public <K> K saveAndGetKey(R object, Class<K> kClass) {

        String allColumns = String.join(", ", table.allColumns());
        String nQuestionMarks = String.join(", ", Collections.nCopies(this.table.allColumns().size(), "?"));
        String INSERT_INTO = String.format("INSERT INTO %s (%s) VALUES (%s)", fQTableName(), allColumns, nQuestionMarks);

        log.info("Saving {} object into table '{}'", tClassName(), fQTableName());
        KeyHolder keyHolder = new GeneratedKeyHolder();

        /*
        SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(jdbcTemplate);
        simpleJdbcInsert.withTableName("USER").usingGeneratedKeyColumns("ID");
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("USERNAME", user.getUserName())
                .addValue("PASSWORD", user.getPassword())
                .addValue("CREATEDTIME", user.getCreatedTime())
                .addValue("UPDATEDTIME", user.getUpdatedTime())
                .addValue("USERTYPE", user.getUserType())
                .addValue("DATEOFBIRTH", user.getDateofBirth());

        Number id = simpleJdbcInsert.executeAndReturnKey(params);
        id.longValue();
        */
        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement(INSERT_INTO, Statement.RETURN_GENERATED_KEYS);
            return object.getPreparedStatement(ps);
        }, keyHolder);

        //K key = keyHolder.getKeyAs(kClass);
        //log.info("Saved object {} to table '{}'. Generated key: {}", tClassName(), fQTableName(), key);

         //*/

        jdbcTemplate.update(INSERT_INTO, object.allValues(), keyHolder, table.primaryKeyColumns().toArray(new String[]{"request_id"}));
        K key = keyHolder.getKeyAs(kClass);
        log.info("Saved object {} to table '{}'. Generated key: {}", tClassName(), fQTableName(), key);
        return key;
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

        String allColumns = String.join(", ", table.allColumns());
        String nQuestionMarks = String.join(", ", Collections.nCopies(this.table.allColumns().size(), "?"));
        log.info("Saving {} {} object(s) into table '{}'", rList.size(), tClassName, fQTableName());
        String INSERT_BATCH_INTO = String.format("INSERT INTO %s (%s) VALUES (%s)", fQTableName(), allColumns, nQuestionMarks);
        jdbcTemplate.batchUpdate(INSERT_BATCH_INTO, rList.stream().map(DRLGDRecord::allValues).collect(Collectors.toList()));
        log.info("Saved {} {} object(s) into table '{}'", rList.size(), tClassName, fQTableName());
    }
}
