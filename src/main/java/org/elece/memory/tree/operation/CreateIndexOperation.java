package org.elece.memory.tree.operation;

import org.elece.config.IDbConfig;
import org.elece.memory.KvSize;
import org.elece.memory.error.BTreeException;
import org.elece.memory.tree.node.AbstractTreeNode;
import org.elece.memory.tree.node.data.IBinaryObject;
import org.elece.storage.error.StorageException;
import org.elece.storage.index.session.IAtomicIOSession;

public class CreateIndexOperation<K, V> {
    private final IDbConfig dbConfig;
    private final IAtomicIOSession<K> atomicIOSession;
    private final IBinaryObject<K> binaryObjectKey;
    private final IBinaryObject<V> binaryObjectValue;
    private final KvSize kvSize;

    public CreateIndexOperation(IDbConfig dbConfig, IAtomicIOSession<K> atomicIOSession, IBinaryObject<K> binaryObjectKey, IBinaryObject<V> binaryObjectValue, KvSize kvSize) {
        this.dbConfig = dbConfig;
        this.atomicIOSession = atomicIOSession;
        this.binaryObjectKey = binaryObjectKey;
        this.binaryObjectValue = binaryObjectValue;
        this.kvSize = kvSize;
    }

    public AbstractTreeNode<K> addIndex(AbstractTreeNode<K> root, K identifier, V value) throws BTreeException, StorageException {
        return null;
    }
}
