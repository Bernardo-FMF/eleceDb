package org.elece.index;

import org.elece.exception.btree.BTreeException;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.storage.StorageException;
import org.elece.memory.tree.node.LeafTreeNode;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public interface IndexManager<K extends Comparable<K>, V> {
    void addIndex(K identifier, V value) throws BTreeException, StorageException, SerializationException;

    void updateIndex(K identifier, V value) throws BTreeException, StorageException, SerializationException;

    Optional<V> getIndex(K identifier) throws BTreeException, StorageException;

    boolean removeIndex(K identifier) throws BTreeException, StorageException, SerializationException;

    void purgeIndex() throws IOException, ExecutionException, InterruptedException;

    int getIndexId();

    LockableIterator<LeafTreeNode.KeyValue<K, V>> getSortedIterator() throws StorageException;
}
