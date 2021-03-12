package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.common.SaveBatchDao;
import it.luca.lgd.jdbc.common.SaveDao;
import it.luca.lgd.jdbc.common.SaveWithGeneratedKeyDao;
import it.luca.lgd.jdbc.record.OozieActionRecord;
import it.luca.lgd.jdbc.record.OozieJobRecord;
import it.luca.lgd.jdbc.record.RequestRecord;
import lombok.extern.slf4j.Slf4j;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.postgres.PostgresPlugin;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
public class DRLGDDao {

    @Autowired
    private DataSource dataSource;

    private Jdbi jdbi;

    @PostConstruct
    private void initJdbi() {

        String jdbiClass = Jdbi.class.getName();
        log.info("Initializing {}", jdbiClass);
        jdbi = Jdbi.create(dataSource)
                .installPlugin(new SqlObjectPlugin())
                .installPlugin(new PostgresPlugin());

        log.info("Initialized {}", jdbiClass);
    }

    private <R, D extends SaveBatchDao<R>> void saveBatch(List<R> rList, Class<R> rClass, Class<D> daoClass) {

        String rClassName = rClass.getSimpleName();
        String daoClassName = daoClass.getSimpleName();
        int listSize = rList.size();
        log.info("Saving {} {} object(s) using {}", listSize, rClassName, daoClassName);
        jdbi.useHandle(handle -> handle.attach(daoClass).save(rList));
        log.info("Saved {} {} object(s) using {}", listSize, rClassName, daoClassName);
    }

    private <R, D extends SaveWithGeneratedKeyDao<R>> R saveObjectWithGeneratedKey(R object, Class<R> rClass, Class<D> daoClass) {

        String rClassName = rClass.getSimpleName();
        String daoClassName = daoClass.getSimpleName();
        log.info("Saving {} object using {}", rClassName, daoClassName);
        R output = jdbi.withHandle(handle -> handle.attach(daoClass).save(object));
        log.info("Saved {} object using {}", rClassName, daoClassName);
        return output;
    }

    private <R, D extends SaveDao<R>> void save(R object, Class<R> rClass, Class<D> daoClass) {

        String rClassName = rClass.getSimpleName();
        String daoClassName = daoClass.getSimpleName();
        log.info("Saving {} object using {}", rClassName, daoClassName);
        jdbi.useHandle(handle -> handle.attach(daoClass).save(object));
        log.info("Saved {} object using {}", rClassName, daoClassName);
    }

    public void saveOozieJobRecord(OozieJobRecord oozieJobRecord) {

        save(oozieJobRecord, OozieJobRecord.class, OozieJobDao.class);
    }

    public void saveOozieActions(List<OozieActionRecord> oozieActionRecords) {

        saveBatch(oozieActionRecords, OozieActionRecord.class, OozieActionDao.class);
    }

    public RequestRecord saveRequestRecord(RequestRecord requestRecord) {

        return saveObjectWithGeneratedKey(requestRecord, RequestRecord.class, RequestDao.class);
    }

    public Optional<OozieJobRecord> findOozieJob(String workflowJobId) {

        return jdbi.withHandle(handle -> handle.attach(OozieJobDao.class)
                .findById(workflowJobId));
    }

    public List<OozieActionRecord> findOozieJobActions(String workflowJobId) {

        return jdbi.withHandle(handle -> handle.attach(OozieActionDao.class)
                .findByLauncherId(workflowJobId));
    }
}
