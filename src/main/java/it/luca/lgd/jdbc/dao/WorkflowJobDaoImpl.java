package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.table.OozieJobTableDefinition;
import it.luca.lgd.model.jdbc.OozieJobRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.oozie.client.WorkflowJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Slf4j
@Repository
public class WorkflowJobDaoImpl extends WorkflowJobDao {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public Optional<OozieJobRecord> findById(String id) {

        String SELECT_OOZIE_JOB_WITH_ID = String.format("SELECT * FROM %s WHERE %s = '%s'", fQTableName(), OozieJobTableDefinition.JOB_LAUNCHER_ID, id);
        return Optional.ofNullable(jdbcTemplate.queryForObject(SELECT_OOZIE_JOB_WITH_ID, OozieJobRecord.class));
    }

    @Override
    public void save(OozieJobRecord object) {

        String INSERT_INTO_OOZIE_JOB = String.format("INSERT INTO %s (, , , , , ,) VALUES ()", fQTableName());
    }

    @Override
    public Optional<OozieJobRecord> lastOozieJob() {
        return Optional.empty();
    }

    @Override
    public Optional<OozieJobRecord> lastOozieJobWithStatus(WorkflowJob.Status status) {
        return Optional.empty();
    }
}
