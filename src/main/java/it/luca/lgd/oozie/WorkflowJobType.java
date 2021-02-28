package it.luca.lgd.oozie;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum WorkflowJobType {

    WORKFLOW_JOB("WORKFLOW_JOB");

    private final String type;
}
