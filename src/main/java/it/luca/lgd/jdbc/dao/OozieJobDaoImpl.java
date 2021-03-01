package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.mapper.OozieJobRecordExtractor;
import it.luca.lgd.jdbc.model.OozieJobRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.oozie.client.WorkflowJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
@Repository
public class OozieJobDaoImpl extends OozieJobDao {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public Optional<OozieJobRecord> findById(String id) {

        String SELECT_OOZIE_JOB_WITH_ID = String.format("SELECT * FROM %s WHERE %s = '%s'", fQTableName(), tableDefinition.JOB_LAUNCHER_ID, id);
        return Optional.ofNullable(jdbcTemplate.query(SELECT_OOZIE_JOB_WITH_ID, new OozieJobRecordExtractor()));
    }

    @Override
    public void save(OozieJobRecord object) {

        String objectId = object.getJobLauncherId();
        log.info("Saving {} object with {} = '{}'", this.tClassName(), tableDefinition.JOB_LAUNCHER_ID, objectId);
        int columnSize = this.tableDefinition.allColumns().size();
        String INSERT_INTO_OOZIE_JOB = String.format("INSERT INTO %s (%s) VALUES (%s)",
                fQTableName(), this.allColumnsSeparatedByComma(), this.nQuestionMarksSeparatedByComma(columnSize));
        jdbcTemplate.update(INSERT_INTO_OOZIE_JOB, object.getJobLauncherId(), object.getJobType(), object.getJobName(), object.getJobUser(),
                object.getJobStatus(), object.getJobStartDate(), object.getJobStartTime(), object.getJobEndDate(), object.getJobEndTime(),
                object.getJobTotalActions(), object.getJobCompletedActions(), object.getJobTrackingUrl(), object.getRecordInsertTime(),
                object.getLastRecordUpdateTime());
        log.info("Successfully saved {} object with {} = '{}'", this.tClassName(), tableDefinition.JOB_LAUNCHER_ID, objectId);
    }

    @Override
    public List<OozieJobRecord> lastNOozieJobs(int n) {

        String SELECT_LAST_N_JOB = String.format("SELECT * FROM %s ORDER BY %s DESC LIMIT %s", fQTableName(), tableDefinition.JOB_START_TIME, n);
        return n == 1 ?
                Collections.singletonList(jdbcTemplate.query(SELECT_LAST_N_JOB, new OozieJobRecordExtractor())) :
                jdbcTemplate.queryForList(SELECT_LAST_N_JOB, OozieJobRecord.class);
    }

    @Override
    public Optional<OozieJobRecord> lastOozieJobWithStatus(WorkflowJob.Status status) {

        String SELECT_LAST_JOB_WITH_STATUS = String.format("SELECT * FROM %s WHERE %s = '%s' ORDER BY %s DESC LIMIT 1",
                fQTableName(), tableDefinition.JOB_STATUS, status.toString(), tableDefinition.JOB_START_TIME);
        return Optional.ofNullable(jdbcTemplate.query(SELECT_LAST_JOB_WITH_STATUS, new OozieJobRecordExtractor()));
    }
}
