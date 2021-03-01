package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.table.TableDefinition;
import lombok.AllArgsConstructor;

import java.util.Collections;
import java.util.Optional;

@AllArgsConstructor
public abstract class BaseDao<R extends TableDefinition<T>, T, K> {

    protected final R tableDefinition;

    public String tClassName() {
        return tableDefinition.getTClass().getName();
    }

    public String fQTableName() {
        return tableDefinition.fQTableName();
    }

    public String allColumnsSeparatedByComma() {

        return String.join(", ", tableDefinition.allColumns());
    }

    public String nQuestionMarksSeparatedByComma(int n) {

        return String.join(", ", Collections.nCopies(n, "?"));
    }

    public abstract Optional<T> findById(K id);

    public abstract void save(T object);
}
