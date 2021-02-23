package it.luca.lgd.oozie.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.oozie.client.WorkflowAction;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class WorkflowActionJsonSerializer extends StdSerializer<WorkflowAction> {

    public WorkflowActionJsonSerializer() {
        super(WorkflowAction.class);
    }

    @Override
    public void serialize(WorkflowAction workflowAction, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("actionName", workflowAction.getName());
        jsonGenerator.writeStringField("type", workflowAction.getType());
        jsonGenerator.writeNumberField("startTime", workflowAction.getStartTime().getTime());
        jsonGenerator.writeStringField("StartTimeTs", simpleDateFormat.format(workflowAction.getStartTime()));
        jsonGenerator.writeNumberField("endTime", workflowAction.getEndTime().getTime());
        jsonGenerator.writeStringField("endTimeTs", simpleDateFormat.format(workflowAction.getEndTime()));
        jsonGenerator.writeStringField("status", workflowAction.getStatus().toString());
        jsonGenerator.writeStringField("trackingUrl", workflowAction.getConsoleUrl());
        jsonGenerator.writeEndObject();
    }
}
