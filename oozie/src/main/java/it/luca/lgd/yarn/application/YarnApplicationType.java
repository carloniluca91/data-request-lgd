package it.luca.lgd.yarn.application;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum YarnApplicationType {

    MAPREDUCE("MAPREDUCE");

    private final String type;
}
