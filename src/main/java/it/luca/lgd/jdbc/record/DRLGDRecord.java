package it.luca.lgd.jdbc.record;

import lombok.Data;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

@Data
public abstract class DRLGDRecord {

    protected Timestamp tsInsert;
    protected Date dtInsert;

    public PreparedStatement getPreparedStatement(PreparedStatement ps) throws SQLException {
        throw  new UnsupportedOperationException();
    }

    public abstract Object[] allValues();
}
