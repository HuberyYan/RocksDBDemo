package com.ukchukx.rocksdbexample.repository;

import java.util.Optional;

/**
 * @author HuberyYan
 */
public interface KVRepository<K, V> {
    boolean save(K key, V value);

    boolean saveByte(K key, byte[] value);

    Optional<V> find(K key);

    byte[] findByte(K key);

    boolean delete(K key);
}