package it.luca.lgd.jdbc.record;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Getter
@Setter
public abstract class BaseRecord {

    protected LocalDateTime tsInsert;
    protected LocalDate dtInsert;
}
