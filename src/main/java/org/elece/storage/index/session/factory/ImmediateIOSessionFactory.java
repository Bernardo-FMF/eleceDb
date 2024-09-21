package org.elece.storage.index.session.factory;

import org.elece.memory.KeyValueSize;
import org.elece.memory.tree.node.NodeFactory;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.session.AtomicIOSession;
import org.elece.storage.index.session.ImmediateIOSession;

/**
 * Factory implementation that builds or reuses a static instance of itself. It's used to create new instances of {@link ImmediateIOSession}.
 */
public class ImmediateIOSessionFactory implements AtomicIOSessionFactory {
    private static ImmediateIOSessionFactory instance;

    private ImmediateIOSessionFactory() {
    }

    public static synchronized ImmediateIOSessionFactory getInstance() {
        if (instance == null)
            instance = new ImmediateIOSessionFactory();
        return instance;
    }

    @Override
    public <K extends Comparable<K>> AtomicIOSession<K> create(IndexStorageManager indexStorageManager, int indexId, NodeFactory<K> nodeFactory, KeyValueSize keyValueSize) {
        return new ImmediateIOSession<>(indexStorageManager, nodeFactory, indexId, keyValueSize);
    }
}