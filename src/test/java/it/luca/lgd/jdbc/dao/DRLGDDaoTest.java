package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.dao.impl.DRLGDDao;
import it.luca.lgd.jdbc.record.OozieActionRecord;
import it.luca.lgd.jdbc.record.OozieJobRecord;
import it.luca.lgd.jdbc.record.RequestRecord;
import it.luca.lgd.model.CancelledFlightsParameters;
import it.luca.lgd.oozie.WorkflowJobLabel;
import it.luca.lgd.utils.Tuple2;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DRLGDDaoTest {

    @Autowired
    private DRLGDDao drlgdDao;

    @Test
    public void saveRequestRecord() {

        CancelledFlightsParameters cancelledFlightsParameters = new CancelledFlightsParameters();
        cancelledFlightsParameters.setIataCode("IATA_CODE");
        cancelledFlightsParameters.setStartDate("2020-01-01");
        cancelledFlightsParameters.setEndDate("2020-12-31");

        Tuple2<Boolean, String> tuple2 = new Tuple2<>(true, "TEST_ID");
        RequestRecord inputRecord = RequestRecord.from(WorkflowJobLabel.CANCELLED_FLIGHTS, cancelledFlightsParameters, tuple2);
        assertNull(inputRecord.getRequestId());
        RequestRecord savedRecord = drlgdDao.saveRequest(inputRecord);
        assertNotNull(savedRecord.getRequestId());
        assertTrue(drlgdDao.findRequestRecord(savedRecord.getRequestId()).isPresent());
    }

    @Test
    public void saveOozieJobAndActions() {

        OozieJobRecord oozieJobRecord = new OozieJobRecord();
        String jobLauncherid = String.format("LAUNCHER_ID_%s", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss")));
        oozieJobRecord.setJobLauncherId(jobLauncherid);
        oozieJobRecord.setJobName("JOB_TEST_ID");
        oozieJobRecord.setJobAppPath("JOB_TEST_APP_PATH");
        oozieJobRecord.setJobTotalActions(3);
        oozieJobRecord.setJobFinishStatus(WorkflowJob.Status.SUCCEEDED.toString());
        oozieJobRecord.setJobStartTime(LocalDateTime.now().minusMinutes(10));
        oozieJobRecord.setJobStartDate(LocalDateTime.now().minusMinutes(10).toLocalDate());
        oozieJobRecord.setJobEndTime(LocalDateTime.now());
        oozieJobRecord.setJobEndDate(LocalDate.now());
        oozieJobRecord.setJobTrackingUrl("TRACKING_URL");
        oozieJobRecord.setTsInsert(LocalDateTime.now());
        oozieJobRecord.setDtInsert(LocalDate.now());

        drlgdDao.saveOozieJob(oozieJobRecord);
        assertTrue(drlgdDao.findOozieJob(jobLauncherid).isPresent());

        OozieActionRecord oozieActionRecord = new OozieActionRecord();

        String actionId = String.format("ACTION_%s", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss")));
        oozieActionRecord.setActionId(actionId);
        oozieActionRecord.setJobLauncherId(jobLauncherid);
        oozieActionRecord.setActionType("PIG");
        oozieActionRecord.setActionName(actionId);
        oozieActionRecord.setActionNumber(1);
        oozieActionRecord.setActionFinishStatus(WorkflowAction.Status.OK.toString());
        oozieActionRecord.setActionStartTime(LocalDateTime.now().minusMinutes(5));
        oozieActionRecord.setActionStartDate(LocalDateTime.now().minusMinutes(1).toLocalDate());
        oozieActionRecord.setActionEndTime(LocalDateTime.now());
        oozieActionRecord.setActionEndDate(LocalDate.now());
        oozieActionRecord.setTsInsert(LocalDateTime.now());
        oozieActionRecord.setDtInsert(LocalDate.now());

        drlgdDao.saveOozieActions(Collections.singletonList(oozieActionRecord));
        assertFalse(drlgdDao.findOozieJobActions(jobLauncherid).isEmpty());
    }
}