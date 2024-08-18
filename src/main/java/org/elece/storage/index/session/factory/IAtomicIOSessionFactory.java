package org.elece.storage.index.session.factory;

import org.elece.memory.KvSize;
import org.elece.memory.tree.node.INodeFactory;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.session.IAtomicIOSession;

public interface IAtomicIOSessionFactory {
    <K> IAtomicIOSession<K> create(IndexStorageManager indexStorageManager, int indexId, INodeFactory<K> nodeFactory, KvSize kvSize);
}
