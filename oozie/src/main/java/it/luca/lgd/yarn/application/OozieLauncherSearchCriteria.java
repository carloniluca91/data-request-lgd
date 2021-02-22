package it.luca.lgd.yarn.application;

public class OozieLauncherSearchCriteria extends ApplicationSearchCriteria{

    public OozieLauncherSearchCriteria(String userName, String applicationQueue, String workflowJobName) {
        super(userName, YarnApplicationType.MAPREDUCE, s -> s.contains(workflowJobName), applicationQueue);
    }
}
