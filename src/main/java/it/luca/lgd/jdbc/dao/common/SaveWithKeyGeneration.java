package it.luca.lgd.jdbc.dao.common;

public interface SaveWithKeyGeneration<R> {

    R save(R object);
}
