package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.record.OozieActionRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.util.List;

@Slf4j
@Repository
public class OozieActionDaoImpl extends OozieActionDao {

    @Override
    public List<OozieActionRecord> getOozieJobActions(String workflowJobId) {

        String SELECT_ACTIONS_OF_JOB = String.format("SELECT * FROM %s WHERE %s = '%s' ORDER BY %s DESC",
                fQTableName(), table.JOB_LAUNCHER_ID, workflowJobId, table.ACTION_START_TIME);
        return jdbcTemplate.queryForList(SELECT_ACTIONS_OF_JOB, OozieActionRecord.class);
    }
}
