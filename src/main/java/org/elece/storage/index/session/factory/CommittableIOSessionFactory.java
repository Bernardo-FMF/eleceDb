package org.elece.storage.index.session.factory;

import org.elece.memory.KeyValueSize;
import org.elece.memory.tree.node.NodeFactory;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.session.AtomicIOSession;
import org.elece.storage.index.session.CommittableIOSession;

/**
 * Factory implementation that builds or reuses a static instance of itself. It's used to create new instances of {@link CommittableIOSession}.
 */
public class CommittableIOSessionFactory implements AtomicIOSessionFactory {
    private static CommittableIOSessionFactory instance;

    private CommittableIOSessionFactory() {
    }

    public static synchronized CommittableIOSessionFactory getInstance() {
        if (instance == null)
            instance = new CommittableIOSessionFactory();
        return instance;
    }

    @Override
    public <K extends Comparable<K>> AtomicIOSession<K> create(IndexStorageManager indexStorageManager, int indexId, NodeFactory<K> nodeFactory, KeyValueSize keyValueSize) {
        return new CommittableIOSession<>(indexStorageManager, nodeFactory, indexId, keyValueSize);
    }
}