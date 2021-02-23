package it.luca.lgd.oozie.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.oozie.client.WorkflowJob;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class WorkflowJobJsonSerializer extends StdSerializer<WorkflowJob> {

    public WorkflowJobJsonSerializer() {
        super(WorkflowJob.class);
    }

    @Override
    public void serialize(WorkflowJob workflowJob, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("workflowJobName", workflowJob.getAppName());
        jsonGenerator.writeNumberField("startTime", workflowJob.getStartTime().getTime());
        jsonGenerator.writeStringField("startTimeTs", simpleDateFormat.format(workflowJob.getStartTime()));
        jsonGenerator.writeNumberField("endTime", workflowJob.getEndTime().getTime());
        jsonGenerator.writeStringField("endTimeTs", simpleDateFormat.format(workflowJob.getEndTime()));
        jsonGenerator.writeStringField("status", workflowJob.getStatus().toString());
        jsonGenerator.writeStringField("trackingUrl", workflowJob.getConsoleUrl());
        jsonGenerator.writeEndObject();
    }
}
