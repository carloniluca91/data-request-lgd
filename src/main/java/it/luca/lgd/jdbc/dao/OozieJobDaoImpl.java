package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.record.OozieJobRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.oozie.client.WorkflowJob;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
@Repository
public class OozieJobDaoImpl extends OozieJobDao {

    @Override
    public List<OozieJobRecord> lastNOozieJobs(int n) {

        String SELECT_LAST_N_JOB = String.format("SELECT * FROM %s ORDER BY %s DESC LIMIT %s", fQTableName(), table.JOB_START_TIME, n);
        return n == 1 ?
                Collections.singletonList(jdbcTemplate.query(SELECT_LAST_N_JOB, table.getResultSetExtractor())) :
                jdbcTemplate.queryForList(SELECT_LAST_N_JOB, OozieJobRecord.class);
    }

    @Override
    public Optional<OozieJobRecord> lastOozieJobWithStatus(WorkflowJob.Status status) {

        String SELECT_LAST_JOB_WITH_STATUS = String.format("SELECT * FROM %s WHERE %s = '%s' ORDER BY %s DESC LIMIT 1",
                fQTableName(), table.JOB_STATUS, status.toString(), table.JOB_START_TIME);
        return Optional.ofNullable(jdbcTemplate.query(SELECT_LAST_JOB_WITH_STATUS, table.getResultSetExtractor()));
    }
}
