package it.luca.lgd.jdbc.dao;

import java.util.Optional;

public interface Dao<R, K> {

    R save(R bean);

    Optional<R> findById(K key);
}
