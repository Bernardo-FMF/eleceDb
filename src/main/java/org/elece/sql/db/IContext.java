package org.elece.sql.db;

public interface IContext<K, V> {
    V findMetadata(K key);
    boolean contains(K key);
    void insert(K key, V value);
    void invalidate(K key);
}
