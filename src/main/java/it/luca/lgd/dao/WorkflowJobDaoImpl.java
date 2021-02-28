package it.luca.lgd.dao;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Slf4j
@Repository
public class WorkflowJobDaoImpl {//implements WorkflowJobDao {

    @Value("${jdbc.postgresql.schema}")
    private String schema;

    @Value("${jdbc.postgresql.oozieJob.table}")
    private String tableName;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private String fQDNTable() {
        return schema + "." + tableName;
    }

    /*
    @Override
    public WorkflowJobRecord findById(String workflowJobId) {

        String SELECT_WORKFLOW_JOB_BY_ID = String.format("SELECT * FROM %s WHERE job_launcher_id = '%s'", fQDNTable(), workflowJobId);
        return jdbcTemplate.queryForObject(SELECT_WORKFLOW_JOB_BY_ID, WorkflowJobRecord.class);
    }

    @Override
    public void insert(WorkflowJobRecord workflowJobRecord) {

        String INSERT_INTO = "INSERT INTO %s.%s ()";

    }

    @Override
    public void updateCompletedActionsAndStatus(String workflowJobId, int completedActions, WorkflowJob.Status status) {

    }

     */
}
