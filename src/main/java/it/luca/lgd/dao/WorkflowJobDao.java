package it.luca.lgd.dao;

import it.luca.lgd.model.jdbc.WorkflowJobRecord;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WorkflowJobDao extends JpaRepository<WorkflowJobRecord, String> {

}
