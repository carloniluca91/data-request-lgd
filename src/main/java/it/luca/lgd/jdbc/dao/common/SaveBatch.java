package it.luca.lgd.jdbc.dao.common;

import java.util.List;

public interface SaveBatch<R> {

    void save(List<R> rList);
}
