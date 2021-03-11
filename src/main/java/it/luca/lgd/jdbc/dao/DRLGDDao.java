package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.record.OozieActionRecord;
import it.luca.lgd.jdbc.record.OozieJobRecord;
import it.luca.lgd.jdbc.record.RequestRecord;
import lombok.extern.slf4j.Slf4j;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.extension.ExtensionCallback;
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

        String jdbiClass = jdbi.getClass().getName();
        log.info("Initializing {}", jdbiClass);
        jdbi = Jdbi.create(dataSource)
                .installPlugin(new SqlObjectPlugin())
                .installPlugin(new PostgresPlugin());

        log.info("Initialized {}", jdbiClass);
    }

    private <R, K, D extends Dao<R, K>> List<R> save(List<R> rList, Class<R> rClass, Class<D> daoClass,
                                                     ExtensionCallback<List<R>, D, RuntimeException> extensionCallback) {

        String rClassName = rClass.getSimpleName();
        String daoClassName = daoClass.getSimpleName();
        int listSize = rList.size();
        log.info("Saving {} {} object(s) using {}", listSize, rClassName, daoClassName);
        List<R> output = jdbi.withExtension(daoClass, extensionCallback);
        log.info("Saved {} {} object(s) using {}", listSize, rClass, daoClassName);
        return output;
    }

    private <R, K, D extends Dao<R, K>> R save(R object, Class<R> rClass, Class<D> daoClass) {

        String rClassName = rClass.getSimpleName();
        String daoClassName = daoClass.getSimpleName();
        ExtensionCallback<R, D, RuntimeException> extensionCallback = extension -> extension.save(object);
        log.info("Saving {} object using {}", rClassName, daoClassName);
        R output = jdbi.withExtension(daoClass, extensionCallback);
        log.info("Saved {} object using {}", rClass, daoClassName);
        return output;
    }

    public OozieJobRecord saveOozieJobRecord(OozieJobRecord oozieJobRecord) {

        return save(oozieJobRecord, OozieJobRecord.class, OozieJobDao.class);
    }

    public List<OozieActionRecord> saveOozieActions(List<OozieActionRecord> oozieActionRecords) {

        ExtensionCallback<List<OozieActionRecord>, OozieActionDao, RuntimeException> extensionCallback = d -> d.saveBatch(oozieActionRecords);
        return save(oozieActionRecords, OozieActionRecord.class, OozieActionDao.class, extensionCallback);
    }

    public RequestRecord saveRequestRecord(RequestRecord requestRecord) {

        return save(requestRecord, RequestRecord.class, RequestDao.class);
    }

    public Optional<OozieJobRecord> findOozieJob(String workflowJobId) {

        return jdbi.withExtension(OozieJobDao.class, d -> d.findById(workflowJobId));
    }

    public List<OozieActionRecord> findOozieJobActions(String workflowJobId) {

        return jdbi.withExtension(OozieActionDao.class, d -> d.findByLauncherId(workflowJobId));
    }
}
