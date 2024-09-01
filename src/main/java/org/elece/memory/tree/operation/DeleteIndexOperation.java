package org.elece.memory.tree.operation;

import org.elece.config.DbConfig;
import org.elece.exception.btree.BTreeException;
import org.elece.exception.storage.StorageException;
import org.elece.memory.tree.node.AbstractTreeNode;
import org.elece.memory.tree.node.NodeFactory;
import org.elece.memory.tree.node.data.BinaryObject;
import org.elece.storage.index.session.AtomicIOSession;

public class DeleteIndexOperation<K, V> {
    private final DbConfig dbConfig;
    private final int indexId;
    private final AtomicIOSession<K> atomicIOSession;
    private final BinaryObject<V> binaryObjectValue;
    private final NodeFactory<K> nodeFactory;

    public DeleteIndexOperation(DbConfig dbConfig, AtomicIOSession<K> atomicIOSession, BinaryObject<V> binaryObjectValue, NodeFactory<K> nodeFactory, int indexId) {
        this.dbConfig = dbConfig;
        this.atomicIOSession = atomicIOSession;
        this.binaryObjectValue = binaryObjectValue;
        this.nodeFactory = nodeFactory;
        this.indexId = indexId;
    }

    public boolean removeIndex(AbstractTreeNode<K> root, K identifier) throws BTreeException, StorageException {
        return false;
    }
}
