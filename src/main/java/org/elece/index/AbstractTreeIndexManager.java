package org.elece.index;

public abstract class AbstractTreeIndexManager<K extends Comparable<K>, V> implements IndexManager<K, V> {
    protected final int indexId;

    public AbstractTreeIndexManager(int indexId) {
        this.indexId = indexId;
    }

    @Override
    public int getIndexId() {
        return this.indexId;
    }
}
