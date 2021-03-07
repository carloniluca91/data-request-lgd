package it.luca.lgd.impala;

import it.luca.lgd.impala.client.ImpalaJDBCClient;
import it.luca.lgd.impala.option.InputOption;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;

@Slf4j
public class Main {

    public static void main(String[] args) {

        log.info("Starting {} class", Main.class.getName());

        // Options for parsing provided input args
        Option urlOption = InputOption.URL.toOption(false);
        Option statementsOption = InputOption.STATEMENTS.toOption(true);

        Options options = new Options();
        options.addOption(urlOption);
        options.addOption(statementsOption);

        try {

            // Parse provided input args
            CommandLineParser parser = new DefaultParser();
            CommandLine commandLine = parser.parse(options, args);
            String url = commandLine.getOptionValue(InputOption.URL.getShortOption());
            String[] statements = commandLine.getOptionValues(InputOption.STATEMENTS.getShortOption());

            // Instantiate Impala client and execute provided statements
            log.info("{}: {}", InputOption.URL.getDescription(), url);
            log.info("Parsed {} statement(s) to execute. Starting to execute them one by one", statements.length);
            ImpalaJDBCClient impalaJDBCClient = new ImpalaJDBCClient(url);
            for (String statement: statements) {

                impalaJDBCClient.executeStatement(statement);
            }

            impalaJDBCClient.close();

        } catch (Exception e) {
            log.error("Caght an exception! Stack trace: ", e);
        }
    }
}
