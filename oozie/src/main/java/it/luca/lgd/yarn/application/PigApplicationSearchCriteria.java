package it.luca.lgd.yarn.application;

public class PigApplicationSearchCriteria extends ApplicationSearchCriteria {

    public PigApplicationSearchCriteria(String userName, String applicationQueue, String pigScriptName) {
        super(userName, YarnApplicationType.MAPREDUCE, s -> s.equalsIgnoreCase(String.format("PigLatin:%s", pigScriptName)), applicationQueue);
    }
}
