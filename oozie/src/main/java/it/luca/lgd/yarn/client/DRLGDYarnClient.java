package it.luca.lgd.yarn.client;

import it.luca.lgd.yarn.application.ApplicationSearchCriteria;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

@Slf4j
public class DRLGDYarnClient {

    private final YarnClient yarnClient = YarnClient.createYarnClient();

    public DRLGDYarnClient(String resouceManagerHostname) {

        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        yarnConfiguration.set("yarn.resourcemanager.hostname", resouceManagerHostname);
        yarnClient.init(yarnConfiguration);
        yarnClient.start();
        String resourceManagerAddress = yarnConfiguration.get("yarn.resourcemanager.address");
        log.info("Successfully created {} connection to {}", YarnClient.class.getSimpleName(), resourceManagerAddress);
    }

    public Optional<ApplicationId> getApplicationId(ApplicationSearchCriteria applicationSearchCriteria,
                                                    List<YarnApplicationState> applicationStates) throws IOException, YarnException {

        Predicate<ApplicationReport> predicate = applicationReport ->
                applicationReport.getStartTime() > applicationSearchCriteria.getStartTime() &&
                        applicationReport.getUser().equalsIgnoreCase(applicationSearchCriteria.getUserName()) &&
                        applicationReport.getApplicationType().equalsIgnoreCase(applicationSearchCriteria.getApplicationType().getType()) &&
                        applicationSearchCriteria.getApplicationNamePredicate().test(applicationReport.getName()) &&
                        applicationReport.getQueue().equalsIgnoreCase(applicationSearchCriteria.getApplicationQueue());

        return yarnClient.getApplications().stream()
                .filter(predicate)
                .min(Comparator.comparing(ApplicationReport::getStartTime))
                .map(ApplicationReport::getApplicationId);
    }
}
