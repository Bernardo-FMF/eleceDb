package org.elece.storage.index.session.factory;

import org.elece.memory.KeyValueSize;
import org.elece.memory.tree.node.NodeFactory;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.session.AtomicIOSession;

public interface AtomicIOSessionFactory {
    <K> AtomicIOSession<K> create(IndexStorageManager indexStorageManager, int indexId, NodeFactory<K> nodeFactory, KeyValueSize keyValueSize);
}
