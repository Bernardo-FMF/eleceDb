package org.elece.memory.tree.operation;

import org.elece.config.IDbConfig;
import org.elece.memory.error.BTreeException;
import org.elece.memory.tree.node.AbstractTreeNode;
import org.elece.memory.tree.node.INodeFactory;
import org.elece.memory.tree.node.data.IBinaryObject;
import org.elece.storage.error.StorageException;
import org.elece.storage.index.session.IAtomicIOSession;

public class DeleteIndexOperation<K, V> {
    private final IDbConfig dbConfig;
    private final int indexId;
    private final IAtomicIOSession<K> atomicIOSession;
    private final IBinaryObject<V> binaryObjectValue;
    private final INodeFactory<K> nodeFactory;

    public DeleteIndexOperation(IDbConfig dbConfig, IAtomicIOSession<K> atomicIOSession, IBinaryObject<V> binaryObjectValue, INodeFactory<K> nodeFactory, int indexId) {
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
