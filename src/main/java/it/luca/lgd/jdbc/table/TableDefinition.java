package it.luca.lgd.jdbc.table;

import it.luca.lgd.exception.IllegalTableEntityException;
import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.persistence.Table;
import java.util.List;
import java.util.Optional;

@AllArgsConstructor
public abstract class TableDefinition<T> {

    @Getter protected final Class<T> tClass;

    public abstract List<String> allColumns();

    public String schema() {

        return Optional.ofNullable(tClass.getAnnotation(Table.class))
                .map(Table::schema)
                .orElseThrow(() -> new IllegalTableEntityException(tClass, "schema"));
    }

    public String tableName() {

        return Optional.ofNullable(tClass.getAnnotation(Table.class))
                .map(Table::name)
                .orElseThrow(() -> new IllegalTableEntityException(tClass, "name"));
    }

    public String fQTableName() {
        return schema() + "." + tableName();
    }
}
