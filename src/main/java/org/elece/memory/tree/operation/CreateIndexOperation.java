package org.elece.memory.tree.operation;

import org.elece.config.DbConfig;
import org.elece.exception.btree.BTreeException;
import org.elece.exception.storage.StorageException;
import org.elece.memory.KeyValueSize;
import org.elece.memory.tree.node.AbstractTreeNode;
import org.elece.memory.tree.node.data.BinaryObject;
import org.elece.storage.index.session.AtomicIOSession;

public class CreateIndexOperation<K, V> {
    private final DbConfig dbConfig;
    private final AtomicIOSession<K> atomicIOSession;
    private final BinaryObject<K> binaryObjectKey;
    private final BinaryObject<V> binaryObjectValue;
    private final KeyValueSize keyValueSize;

    public CreateIndexOperation(DbConfig dbConfig, AtomicIOSession<K> atomicIOSession, BinaryObject<K> binaryObjectKey, BinaryObject<V> binaryObjectValue, KeyValueSize keyValueSize) {
        this.dbConfig = dbConfig;
        this.atomicIOSession = atomicIOSession;
        this.binaryObjectKey = binaryObjectKey;
        this.binaryObjectValue = binaryObjectValue;
        this.keyValueSize = keyValueSize;
    }

    public AbstractTreeNode<K> addIndex(AbstractTreeNode<K> root, K identifier, V value) throws BTreeException, StorageException {
        return null;
    }
}
