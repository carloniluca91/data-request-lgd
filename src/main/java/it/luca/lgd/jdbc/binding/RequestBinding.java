package it.luca.lgd.jdbc.binding;

import it.luca.lgd.jdbc.record.RequestRecord;
import it.luca.lgd.jdbc.table.RequestTable;
import org.jdbi.v3.sqlobject.customizer.SqlStatementCustomizerFactory;
import org.jdbi.v3.sqlobject.customizer.SqlStatementCustomizingAnnotation;
import org.jdbi.v3.sqlobject.customizer.SqlStatementParameterCustomizer;

import java.lang.annotation.*;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;

import static it.luca.lgd.utils.JsonUtils.objectToString;
import static it.luca.lgd.utils.TimeUtils.toSqlDate;
import static it.luca.lgd.utils.TimeUtils.toSqlTimestamp;

@SqlStatementCustomizingAnnotation(RequestBinding.RequestRecordBindigFactory.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER})
public @interface RequestBinding {

    class RequestRecordBindigFactory implements SqlStatementCustomizerFactory {

        @Override
        public SqlStatementParameterCustomizer createForParameter(
                Annotation annotation, Class<?> sqlObjectType, Method method, Parameter param, int index, Type paramType) {

            return (q, arg) -> {
                RequestRecord requestRecord = (RequestRecord) arg;
                q.bind(RequestTable.REQUEST_USER, requestRecord.getRequestUser());
                q.bind(RequestTable.JOB_ID, requestRecord.getWorkflowJobLabel().getId());
                q.bind(RequestTable.REQUEST_TIME, toSqlTimestamp(requestRecord.getRequestTime()));
                q.bind(RequestTable.REQUEST_DATE, toSqlDate(requestRecord.getRequestDate()));
                q.bind(RequestTable.REQUEST_PARAMETERS, objectToString(requestRecord.getRequestParameters()));
                q.bind(RequestTable.JOB_LAUNCHER_ID, requestRecord.getJobLauncherId());
                q.bind(RequestTable.JOB_SUBMISSION_CODE, requestRecord.getJobSubmissionCode());
                q.bind(RequestTable.JOB_SUBMISSION_ERROR, requestRecord.getJobSubmissionError());
                q.bind(RequestTable.TS_INSERT, toSqlTimestamp(requestRecord.getTsInsert()));
                q.bind(RequestTable.DT_INSERT, toSqlDate(requestRecord.getDtInsert()));
            };
        }
    }
}