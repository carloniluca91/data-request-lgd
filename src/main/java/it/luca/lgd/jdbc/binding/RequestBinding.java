package it.luca.lgd.jdbc.binding;

import it.luca.lgd.jdbc.record.RequestRecord;
import it.luca.lgd.jdbc.table.RequestTable1;
import it.luca.lgd.utils.JsonUtils;
import org.jdbi.v3.sqlobject.customizer.SqlStatementCustomizerFactory;
import org.jdbi.v3.sqlobject.customizer.SqlStatementCustomizingAnnotation;
import org.jdbi.v3.sqlobject.customizer.SqlStatementParameterCustomizer;

import java.lang.annotation.*;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;

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
                q.bind(RequestTable1.REQUEST_USER, requestRecord.getRequestUser());
                q.bind(RequestTable1.JOB_ID, requestRecord.getWorkflowJobId().getId());
                q.bind(RequestTable1.REQUEST_TIME, toSqlTimestamp(requestRecord.getRequestTime()));
                q.bind(RequestTable1.REQUEST_DATE, toSqlDate(requestRecord.getRequestDate()));
                q.bind(RequestTable1.REQUEST_PARAMETERS, JsonUtils.objToString(requestRecord.getRequestParameters()));
                q.bind(RequestTable1.JOB_LAUNCHER_ID, requestRecord.getJobLauncherId());
                q.bind(RequestTable1.JOB_SUBMISSION_CODE, requestRecord.getJobSubmissionCode());
                q.bind(RequestTable1.JOB_SUBMISSION_ERROR, requestRecord.getJobSubmissionError());
            };
        }
    }
}