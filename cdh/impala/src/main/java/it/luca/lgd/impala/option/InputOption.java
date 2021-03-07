package it.luca.lgd.impala.option;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.cli.Option;

@Getter
@AllArgsConstructor
public enum InputOption {

    URL("u", "url", "Impala JDBC connection url"),
    STATEMENTS("c", "commands", "Command(s) to execute. Must be sintactically corrected queries with no return result");

    private final String shortOption;
    private final String longOption;
    private final String description;

    public Option toOption(boolean multipleArgs) {

        Option.Builder builder = Option.builder(shortOption)
                .longOpt(longOption)
                .desc(description)
                .required();

        return multipleArgs ?
                builder.hasArgs().build() :
                builder.hasArg().build();
    }
}