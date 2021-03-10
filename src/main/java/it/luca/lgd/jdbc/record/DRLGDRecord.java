package it.luca.lgd.jdbc.record;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Getter
@Setter
public abstract class DRLGDRecord {

    protected LocalDateTime tsInsert;
    protected LocalDate dtInsert;

    public Object[] allValues() {
        return new Object[]{};
    }
}
