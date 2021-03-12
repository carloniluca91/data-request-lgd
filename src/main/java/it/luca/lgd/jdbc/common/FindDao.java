package it.luca.lgd.jdbc.common;

import java.util.Optional;

public interface FindDao<R, K> {

    Optional<R> findById(K key);

}
