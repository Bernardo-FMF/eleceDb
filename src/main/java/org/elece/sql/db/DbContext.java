package org.elece.sql.db;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DbContext implements IContext<String, TableMetadata>{
    private final Map<String, TableMetadata> tableMetadata;
    private final Queue<String> keyEvictionList;
    private final int cacheSize;

    public DbContext() {
        this(Integer.MAX_VALUE);
    }

    public DbContext(int cacheSize) {
        this.cacheSize = cacheSize;
        this.tableMetadata = new ConcurrentHashMap<>();
        this.keyEvictionList = new ArrayDeque<>();
    }

    @Override
    public TableMetadata findMetadata(String key) {
        return tableMetadata.get(key);
    }

    @Override
    public boolean contains(String key) {
        return tableMetadata.containsKey(key);
    }

    @Override
    public void insert(String key, TableMetadata value) {
        if (tableMetadata.size() >= cacheSize) {
            tableMetadata.remove(keyEvictionList.poll());
        }

        tableMetadata.put(key, value);
        keyEvictionList.add(key);
    }

    @Override
    public void invalidate(String key) {
        tableMetadata.remove(key);
        keyEvictionList.remove(key);
    }
}
