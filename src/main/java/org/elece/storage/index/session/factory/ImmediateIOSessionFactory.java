package org.elece.storage.index.session.factory;

import org.elece.memory.KvSize;
import org.elece.memory.tree.node.INodeFactory;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.session.IAtomicIOSession;
import org.elece.storage.index.session.ImmediateIOSession;

/**
 * Factory implementation that builds or reuses a static instance of itself. It's used to create new instances of {@link ImmediateIOSession}.
 */
public class ImmediateIOSessionFactory implements IAtomicIOSessionFactory {
    private static ImmediateIOSessionFactory instance;

    private ImmediateIOSessionFactory() {
    }

    public static synchronized ImmediateIOSessionFactory getInstance() {
        if (instance == null)
            instance = new ImmediateIOSessionFactory();
        return instance;
    }

    @Override
    public <K> IAtomicIOSession<K> create(IndexStorageManager indexStorageManager, int indexId, INodeFactory<K> nodeFactory, KvSize kvSize) {
        return new ImmediateIOSession<>(indexStorageManager, nodeFactory, indexId, kvSize);
    }
}