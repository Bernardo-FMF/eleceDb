package org.elece.index;

import org.elece.exception.btree.BTreeException;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.storage.StorageException;
import org.elece.memory.tree.node.AbstractTreeNode;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public interface IndexManager<K extends Comparable<K>, V> {
    AbstractTreeNode<K> addIndex(K identifier, V value) throws BTreeException, StorageException, SerializationException;

    AbstractTreeNode<K> updateIndex(K identifier, V value) throws BTreeException, StorageException, SerializationException;

    Optional<V> getIndex(K identifier) throws BTreeException, StorageException;

    boolean removeIndex(K identifier) throws BTreeException, StorageException, SerializationException;

    void purgeIndex() throws IOException, ExecutionException, InterruptedException;

    int getIndexId();
}
