package it.luca.lgd.jdbc.dao;

public interface JDBIDao<R, K> {

    void save(R bean);

    K saveAndGetKey(R bean);

    R findById(K key);
}
