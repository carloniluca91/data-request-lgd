package it.luca.lgd.jdbc.dao.common;

import java.util.Optional;

public interface Find<R, K> {

    Optional<R> findByKey(K key);

}
