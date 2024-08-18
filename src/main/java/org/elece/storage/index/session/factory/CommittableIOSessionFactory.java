package org.elece.storage.index.session.factory;

import org.elece.memory.KvSize;
import org.elece.memory.tree.node.INodeFactory;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.session.CommittableIOSession;
import org.elece.storage.index.session.IAtomicIOSession;

/**
 * Factory implementation that builds or reuses a static instance of itself. It's used to create new instances of {@link CommittableIOSession}.
 */
public class CommittableIOSessionFactory implements IAtomicIOSessionFactory {
    private static CommittableIOSessionFactory instance;

    private CommittableIOSessionFactory() {
    }

    public static synchronized CommittableIOSessionFactory getInstance() {
        if (instance == null)
            instance = new CommittableIOSessionFactory();
        return instance;
    }

    @Override
    public <K> IAtomicIOSession<K> create(IndexStorageManager indexStorageManager, int indexId, INodeFactory<K> nodeFactory, KvSize kvSize) {
        return new CommittableIOSession<>(indexStorageManager, nodeFactory, indexId, kvSize);
    }
}