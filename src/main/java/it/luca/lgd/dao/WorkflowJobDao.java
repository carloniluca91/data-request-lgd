package it.luca.lgd.dao;

import it.luca.lgd.model.jdbc.OozieJobRecord;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WorkflowJobDao extends JpaRepository<OozieJobRecord, String> {

}
