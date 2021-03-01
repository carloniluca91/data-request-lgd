package it.luca.lgd.jdbc.dao;

import it.luca.lgd.jdbc.table.TableDefinition;
import lombok.AllArgsConstructor;

import java.util.Optional;

@AllArgsConstructor
public abstract class BaseDao<T, K, R extends TableDefinition<T>> {

    protected final R tableDefinition;

    public String fQTableName() {
        return tableDefinition.fQTableName();
    }

    public abstract Optional<T> findById(K id);

    public abstract void save(T object);
}
