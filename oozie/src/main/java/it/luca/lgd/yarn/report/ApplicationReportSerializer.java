package it.luca.lgd.yarn.report;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import it.luca.lgd.yarn.utils.TimeUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

import java.io.IOException;
import java.time.ZoneId;

public class ApplicationReportSerializer extends StdSerializer<ApplicationReport> {

    public ApplicationReportSerializer() {
        super(ApplicationReport.class);
    }

    @Override
    public void serialize(ApplicationReport applicationReport, JsonGenerator jgen, SerializerProvider serializerProvider)
            throws IOException {

        ZoneId zoneId = TimeUtils.romeZoneId();

        jgen.writeStartObject();
        jgen.writeStringField("applicationId", applicationReport.getApplicationId().toString());
        jgen.writeStringField("applicationName", applicationReport.getName());
        jgen.writeStringField("applicationType", applicationReport.getApplicationType());
        jgen.writeStringField("user", applicationReport.getUser());
        jgen.writeStringField("queue", applicationReport.getQueue());
        jgen.writeStringField("applicationState", applicationReport.getYarnApplicationState().toString());
        jgen.writeStringField("trackingUrl", applicationReport.getTrackingUrl());
        jgen.writeNumberField("startTime", applicationReport.getStartTime());
        jgen.writeStringField("startTimeFormatted", TimeUtils.epochMillsToZonedTimestamp(applicationReport.getStartTime(), zoneId));
        jgen.writeStringField("timeZone", zoneId.getId());
        jgen.writeNumberField("finishTime", applicationReport.getFinishTime());
        jgen.writeStringField("finishTimeFormatted", TimeUtils.epochMillsToZonedTimestamp(applicationReport.getFinishTime(), zoneId));
        jgen.writeEndObject();
    }
}
