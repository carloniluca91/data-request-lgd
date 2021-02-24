package it.luca.lgd.oozie.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class WorkflowJobJsonSerializer extends StdSerializer<WorkflowJob> {

    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final Function<Date, Long> dateLongFunction = date -> Optional.ofNullable(date).isPresent() ?
            date.getTime() :
            -1;

    private final Function<Date, String> dateStringFunction = date -> Optional.ofNullable(date).isPresent() ?
            simpleDateFormat.format(date) :
            "NotCompleted";

    public WorkflowJobJsonSerializer() {
        super(WorkflowJob.class);
    }

    private void writeWorkflowActionList(JsonGenerator jsonGenerator, String fieldName, List<WorkflowAction> workflowActions) throws IOException {

        if (!workflowActions.isEmpty()) {

            jsonGenerator.writeArrayFieldStart(fieldName);
            for (WorkflowAction workflowAction: workflowActions) {

                jsonGenerator.writeStartObject();
                jsonGenerator.writeStringField("actionName", workflowAction.getName());
                jsonGenerator.writeStringField("type", workflowAction.getType());
                jsonGenerator.writeStringField("actionId", workflowAction.getId());
                jsonGenerator.writeStringField("externalActionId", workflowAction.getExternalId());
                jsonGenerator.writeNumberField("startTime", dateLongFunction.apply(workflowAction.getStartTime()));
                jsonGenerator.writeStringField("startTimeTs", dateStringFunction.apply(workflowAction.getStartTime()));
                jsonGenerator.writeNumberField("endTime", dateLongFunction.apply(workflowAction.getEndTime()));
                jsonGenerator.writeStringField("endTimeTs", dateStringFunction.apply(workflowAction.getEndTime()));
                jsonGenerator.writeStringField("status", workflowAction.getStatus().toString());
                jsonGenerator.writeStringField("errorCode", workflowAction.getErrorCode());
                jsonGenerator.writeStringField("errorMessage", workflowAction.getErrorMessage());
                jsonGenerator.writeStringField("trackingUrl", workflowAction.getConsoleUrl());
                jsonGenerator.writeEndObject();
            }

            jsonGenerator.writeEndArray();
        }
    }

    @Override
    public void serialize(WorkflowJob workflowJob, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {

        List<WorkflowAction.Status> completeStatuses = Arrays.asList(WorkflowAction.Status.DONE,
                WorkflowAction.Status.FAILED,
                WorkflowAction.Status.KILLED,
                WorkflowAction.Status.OK);

        List<WorkflowAction> runningActions = workflowJob.getActions().stream()
                .filter(workflowAction -> workflowAction.getStatus() == WorkflowAction.Status.RUNNING)
                .collect(Collectors.toList());

        List<WorkflowAction> completedActions = workflowJob.getActions().stream()
                .filter(workflowAction -> completeStatuses.contains(workflowAction.getStatus()))
                .collect(Collectors.toList());

        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("workflowJobName", workflowJob.getAppName());
        jsonGenerator.writeStringField("jobId", workflowJob.getId());
        jsonGenerator.writeNumberField("startTime", dateLongFunction.apply(workflowJob.getStartTime()));
        jsonGenerator.writeStringField("startTimeTs", dateStringFunction.apply(workflowJob.getStartTime()));
        jsonGenerator.writeNumberField("endTime", dateLongFunction.apply(workflowJob.getEndTime()));
        jsonGenerator.writeStringField("endTimeTs", dateStringFunction.apply(workflowJob.getEndTime()));
        jsonGenerator.writeStringField("status", workflowJob.getStatus().toString());
        jsonGenerator.writeStringField("trackingUrl", workflowJob.getConsoleUrl());
        writeWorkflowActionList(jsonGenerator, "completedActions", completedActions);
        writeWorkflowActionList(jsonGenerator, "runningAction", runningActions);
        jsonGenerator.writeEndObject();
    }
}
