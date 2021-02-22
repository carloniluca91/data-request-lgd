package it.luca.lgd.yarn.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import it.luca.lgd.yarn.application.ApplicationSearchCriteria;
import it.luca.lgd.yarn.report.ApplicationReportSerializer;
import it.luca.lgd.yarn.utils.TimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;

@Slf4j
public class DRLGDYarnClient {

    private final YarnClient yarnClient = YarnClient.createYarnClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public DRLGDYarnClient(String resouceManagerHostname) {

        // Initialize ObjectMapper (for application report printing)
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(ApplicationReport.class, new ApplicationReportSerializer());
        objectMapper.registerModule(simpleModule);

        // Initialize YarnConfiguration and YarnClient
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        yarnConfiguration.set("yarn.resourcemanager.hostname", resouceManagerHostname);
        yarnClient.init(yarnConfiguration);
        yarnClient.start();

        String resourceManagerAddress = yarnConfiguration.get("yarn.resourcemanager.address");
        log.info("Successfully created {} connection to {}", YarnClient.class.getSimpleName(), resourceManagerAddress);
    }

    public ApplicationId getApplicationId(ApplicationSearchCriteria applicationSearchCriteria) throws IOException, YarnException {

        List<YarnApplicationState> applicationStates = Arrays.asList(YarnApplicationState.ACCEPTED,
                YarnApplicationState.SUBMITTED,
                YarnApplicationState.RUNNING);

        // Predicate for detecting the right applicationId
        Predicate<ApplicationReport> predicate = applicationReport ->
                applicationReport.getUser().equals(applicationSearchCriteria.getUser()) &&
                        applicationReport.getStartTime() >= applicationSearchCriteria.getStartTime() &&
                        applicationReport.getApplicationType().equals(applicationSearchCriteria.getApplicationType().getType()) &&
                        applicationSearchCriteria.getApplicationNamePredicate().test(applicationReport.getName()) &&
                        applicationReport.getQueue().equals(applicationSearchCriteria.getQueue()) &&
                        applicationStates.contains(applicationReport.getYarnApplicationState());

        Optional<ApplicationId> optionalApplicationId = yarnClient.getApplications()
                .stream().filter(predicate)
                .min(Comparator.comparing(ApplicationReport::getStartTime))
                .map(ApplicationReport::getApplicationId);

        String criteria = applicationSearchCriteria.toString();
        if (optionalApplicationId.isPresent()) {
            log.info("Detected an application matching following criteria [{}]", criteria);
        } else {
            log.warn("Unable to detect an application matching following criteria [{}]", criteria);
        }

        return optionalApplicationId.orElse(null);
    }

    public void pollApplicationReport(ApplicationId applicationId) throws IOException, YarnException {

        if (Optional.ofNullable(applicationId).isPresent()) {

            ApplicationReport applicationReport = yarnClient.getApplicationReport(applicationId);
            String applicationReportJson = objectMapper.writeValueAsString(applicationReport);
            log.info("Report for application '{}' at {}:\n\n{}\n", applicationReport.getName(),
                    TimeUtils.epochMillsToZonedTimestamp(System.currentTimeMillis(), TimeUtils.romeZoneId()),
                    applicationReportJson);
        }
    }
}
