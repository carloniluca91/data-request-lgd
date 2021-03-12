package it.luca.lgd.utils;

import it.luca.lgd.exception.IllegalWorkflowParameterException;
import it.luca.lgd.oozie.WorkflowJobParameter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.Optional;

@Slf4j
@NoArgsConstructor
public class JobConfiguration extends PropertiesConfiguration {

    public JobConfiguration(String fileName) throws ConfigurationException {

        super(fileName);
        log.info("Successfully loaded {} file", fileName);
    }

    public String getParameter(WorkflowJobParameter workflowJobParameter) throws IllegalWorkflowParameterException {

        return Optional.ofNullable(super.getString(workflowJobParameter.getName()))
                .orElseThrow(() -> new IllegalWorkflowParameterException(workflowJobParameter));
    }
}
