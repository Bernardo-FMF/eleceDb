package org.elece.index;

import org.elece.config.DbConfig;
import org.elece.memory.Pointer;
import org.elece.memory.data.BinaryObjectFactory;
import org.elece.memory.data.PointerBinaryObject;
import org.elece.memory.tree.node.DefaultNodeFactory;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.session.factory.SessionFactory;

public class ClusterTreeIndexManager<K extends Comparable<K>> extends TreeIndexManager<K, Pointer> {
    public ClusterTreeIndexManager(int indexId, IndexStorageManager indexStorageManager,
                                   SessionFactory iOSessionFactory, DbConfig dbConfig,
                                   BinaryObjectFactory<K> kBinaryObjectFactory) {
        super(indexId, indexStorageManager, iOSessionFactory, dbConfig, kBinaryObjectFactory, new PointerBinaryObject.Factory(), new DefaultNodeFactory<>(kBinaryObjectFactory, new PointerBinaryObject.Factory()));
    }
}
