package it.luca.lgd.jdbc.dao.impl;

import it.luca.lgd.jdbc.dao.common.Find;
import it.luca.lgd.jdbc.dao.common.SaveBatch;
import it.luca.lgd.jdbc.dao.common.Save;
import it.luca.lgd.jdbc.dao.common.SaveWithKeyGeneration;
import it.luca.lgd.jdbc.record.BaseRecord;
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

    /**
     * Init Jdbi instance
     */

    @PostConstruct
    private void initJdbi() {

        String jdbiClass = Jdbi.class.getName();
        log.info("Initializing {}", jdbiClass);
        jdbi = Jdbi.create(dataSource)
                .installPlugin(new SqlObjectPlugin())
                .installPlugin(new PostgresPlugin());

        log.info("Initialized {}", jdbiClass);
    }

    /**
     * Retrieves an optional object of type R using dao type D
     * @param key: object's key
     * @param rClass: object's class
     * @param dClass: dao's class
     * @param <R>: object's type
     * @param <D>: dao's type
     * @param <K>: key's type
     * @return an optional object of type R
     */

    private <R extends BaseRecord, D extends Find<R, K>, K> Optional<R> findByKey(K key, Class<R> rClass, Class<D> dClass) {

        String rClassName = rClass.getSimpleName();
        String dClassName = dClass.getSimpleName();
        log.info("Retrieving (optional) {} object with key {} using {}", rClassName, key, dClassName);
        Optional<R> rOptional = jdbi.withHandle(handle -> handle.attach(dClass).findByKey(key));
        log.info("Retrieved (optional) {} object with key {} using {}", rClassName, key, dClassName);
        return rOptional;
    }

    /**
     * Stores given list of records
     * @param rList: list of records to be storeed
     * @param rClass: record's class
     * @param daoClass: class of DAO to be used
     * @param <R>: record's type
     * @param <D>: dao's type (must extend SaveBatch<R>
     */

    private <R extends BaseRecord, D extends SaveBatch<R>> void saveBatch(List<R> rList, Class<R> rClass, Class<D> daoClass) {

        String rClassName = rClass.getSimpleName();
        String daoClassName = daoClass.getSimpleName();
        int listSize = rList.size();
        log.info("Saving {} {} object(s) using {}", listSize, rClassName, daoClassName);
        jdbi.useHandle(handle -> handle.attach(daoClass).save(rList));
        log.info("Saved {} {} object(s) using {}", listSize, rClassName, daoClassName);
    }

    /**
     * Stores given record and return same record with generated key(s)
     * @param object: record to be storeed
     * @param rClass: record's class
     * @param daoClass: class of Dao to be used
     * @param <R>: record's type
     * @param <D>: dao's type (must extend SaveBatch<R>
     * @return input record with generated key
     */

    private <R extends BaseRecord, D extends SaveWithKeyGeneration<R>> R saveWithKeyGeneration(R object, Class<R> rClass, Class<D> daoClass) {

        String rClassName = rClass.getSimpleName();
        String daoClassName = daoClass.getSimpleName();
        log.info("Saving {} object using {}", rClassName, daoClassName);
        R output = jdbi.withHandle(handle -> handle.attach(daoClass).save(object));
        log.info("Saved {} object using {}", rClassName, daoClassName);
        return output;
    }

    /**
     * Stores given record and return same record with generated key(s)
     * @param object: record to be storeed
     * @param rClass: record's class
     * @param daoClass: class of Dao to be used
     * @param <R>: record's type
     * @param <D>: dao's type (must extend SaveBatch<R>
     */
    
    private <R extends BaseRecord, D extends Save<R>> void save(R object, Class<R> rClass, Class<D> daoClass) {

        String rClassName = rClass.getSimpleName();
        String daoClassName = daoClass.getSimpleName();
        log.info("Saving {} object using {}", rClassName, daoClassName);
        jdbi.useHandle(handle -> handle.attach(daoClass).save(object));
        log.info("Saved {} object using {}", rClassName, daoClassName);
    }

    /**
     * Stores given OozieJobRecord
     * @param oozieJobRecord: record to be stored
     */
    
    public void saveOozieJob(OozieJobRecord oozieJobRecord) {

        save(oozieJobRecord, OozieJobRecord.class, OozieJobDao.class);
    }

    /**
     * Stores given list of OozieActionRecord
     * @param oozieActionRecords: records to be stored
     */

    public void saveOozieActions(List<OozieActionRecord> oozieActionRecords) {

        saveBatch(oozieActionRecords, OozieActionRecord.class, OozieActionDao.class);
    }

    /**
     * Stores given RequestRecord
     * @param requestRecord: record to be stored
     * @return provided RequestRecord with generated key
     */

    public RequestRecord saveRequest(RequestRecord requestRecord) {

        return saveWithKeyGeneration(requestRecord, RequestRecord.class, RequestDao.class);
    }

    /**
     * Retrieves an optional OozieJobRecord for given Oozie job id
     * @param workflowJobId: Oozie job id
     * @return non-empty optional if a record is found
     */

    public Optional<OozieJobRecord> findOozieJob(String workflowJobId) {

        return jdbi.withHandle(handle -> handle.attach(OozieJobDao.class)
                .findByKey(workflowJobId));
    }

    /**
     * Retrieves OozieActionRecords for given Oozie job id
     * @param workflowJobId: Oozie job id
     * @return list of OozieActionRecords (empty if no record was retrieved)
     */

    public List<OozieActionRecord> findOozieJobActions(String workflowJobId) {

        return jdbi.withHandle(handle -> handle.attach(OozieActionDao.class)
                .findByLauncherId(workflowJobId));
    }

    /**
     * Retrieves an optional RequestRecord for given request id
     * @param key: request id
     * @return non-empty optional if a RequestRecord with given key exists
     */

    public Optional<RequestRecord> findRequestRecord(Integer key) {

        return findByKey(key, RequestRecord.class, RequestDao.class);
    }
}
