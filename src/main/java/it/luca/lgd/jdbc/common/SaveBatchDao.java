package it.luca.lgd.jdbc.common;

import java.util.List;

public interface SaveBatchDao<R> {

    void save(List<R> rList);
}
