package it.luca.lgd.jdbc.record;

import lombok.Data;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;

@Data
public abstract class DRLGDRecord implements Serializable {

    protected Timestamp tsInsert;
    protected Date dtInsert;

    public abstract Object[] primaryKeyValues();

    public abstract Object[] allValues();
}
