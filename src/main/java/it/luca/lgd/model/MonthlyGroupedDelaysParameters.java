package it.luca.lgd.model;

import it.luca.lgd.oozie.WorkflowJobParameter;
import it.luca.lgd.utils.Tuple2;

import java.util.Map;

public class MonthlyGroupedDelaysParameters implements JobParameters {

    @Override
    public Tuple2<Boolean, String> validate() {
        return null;
    }

    @Override
    public Map<WorkflowJobParameter, String> toMap() {
        return null;
    }
}
