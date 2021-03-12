package it.luca.lgd.jdbc.common;

import org.jdbi.v3.sqlobject.transaction.Transaction;

public interface SaveWithGeneratedKeyDao<R> {

    @Transaction
    R save(R object);
}
