package it.luca.lgd.jdbc.common;

public interface SaveDao<R> {

    void save(R object);
}
