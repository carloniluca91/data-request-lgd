package it.luca.lgd.impala.option;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.cli.Option;

@Getter
@AllArgsConstructor
public enum InputOption {

    URL("u", "url", "Impala JDBC connection url"),
    STATEMENTS("s", "statements", "Statement(s) to execute. Must be sintactically corrected queries");

    private final String shortOption;
    private final String longOption;
    private final String description;

    public Option toOption(boolean multipleArgs) {

        Option option = new Option(shortOption, longOption, true, description);
        option.setRequired(true);
        if (multipleArgs) {
            option.setArgs(Option.UNLIMITED_VALUES);
        } else {
            option.setArgs(1);
        }

        return option;
    }
}