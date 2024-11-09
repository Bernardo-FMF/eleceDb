package org.elece.storage.index.session.factory;

import org.elece.config.DbConfig;
import org.elece.memory.KeyValueSize;
import org.elece.memory.tree.node.NodeFactory;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.session.CommittableSession;
import org.elece.storage.index.session.ImmediateSession;
import org.elece.storage.index.session.Session;

/**
 * Factory implementation that builds or reuses a static instance of itself. It's used to create new instances of either {@link ImmediateSession} or {@link CommittableSession}.
 */
public class DefaultSessionFactory implements SessionFactory {
    private static DefaultSessionFactory instance;

    private final DbConfig dbConfig;

    private DefaultSessionFactory(DbConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    public static synchronized DefaultSessionFactory getInstance(DbConfig dbConfig) {
        if (instance == null) {
            instance = new DefaultSessionFactory(dbConfig);
        }
        return instance;
    }

    @Override
    public <K extends Comparable<K>> Session<K> create(IndexStorageManager indexStorageManager, int indexId,
                                                       NodeFactory<K> nodeFactory, KeyValueSize keyValueSize) {
        if (dbConfig.getIOSessionStrategy() == DbConfig.SessionStrategy.COMMITTABLE) {
            return new CommittableSession<>(indexStorageManager, nodeFactory, indexId, keyValueSize);
        } else {
            return new ImmediateSession<>(indexStorageManager, nodeFactory, indexId, keyValueSize);
        }
    }
}