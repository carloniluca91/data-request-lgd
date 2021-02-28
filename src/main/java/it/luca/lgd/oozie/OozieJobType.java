package it.luca.lgd.oozie;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum OozieJobType {

    WORKFLOW("WORKFLOW");

    private final String type;
}
