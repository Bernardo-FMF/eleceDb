package org.elece.storage.index.session.factory;

import org.elece.config.DbConfig;
import org.elece.memory.KeyValueSize;
import org.elece.memory.tree.node.NodeFactory;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.session.AtomicIOSession;
import org.elece.storage.index.session.CommittableIOSession;
import org.elece.storage.index.session.ImmediateIOSession;

/**
 * Factory implementation that builds or reuses a static instance of itself. It's used to create new instances of either {@link ImmediateIOSession} or {@link CommittableIOSession}.
 */
public class AtomicIOSessionFactory implements IOSessionFactory {
    private static AtomicIOSessionFactory instance;

    private final DbConfig dbConfig;

    private AtomicIOSessionFactory(DbConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    public static synchronized AtomicIOSessionFactory getInstance(DbConfig dbConfig) {
        if (instance == null) {
            instance = new AtomicIOSessionFactory(dbConfig);
        }
        return instance;
    }

    @Override
    public <K extends Comparable<K>> AtomicIOSession<K> create(IndexStorageManager indexStorageManager, int indexId, NodeFactory<K> nodeFactory, KeyValueSize keyValueSize) {
        if (dbConfig.getIOSessionStrategy() == DbConfig.IOSessionStrategy.COMMITTABLE) {
            return new CommittableIOSession<>(indexStorageManager, nodeFactory, indexId, keyValueSize);
        } else {
            return new ImmediateIOSession<>(indexStorageManager, nodeFactory, indexId, keyValueSize);
        }
    }
}