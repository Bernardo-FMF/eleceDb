package org.elece.index;

import org.elece.exception.*;
import org.elece.memory.tree.node.LeafTreeNode;

import java.util.Optional;

public interface IndexManager<K extends Comparable<K>, V> extends Queryable<K, V> {
    void addIndex(K identifier, V value) throws BTreeException, StorageException, SerializationException,
                                                InterruptedTaskException, FileChannelException;

    void updateIndex(K identifier, V value) throws BTreeException, StorageException, SerializationException,
                                                   InterruptedTaskException, FileChannelException;

    Optional<V> getIndex(K identifier) throws BTreeException, StorageException, InterruptedTaskException,
                                              FileChannelException;

    boolean removeIndex(K identifier) throws BTreeException, StorageException, SerializationException,
                                             InterruptedTaskException, FileChannelException;

    void purgeIndex() throws InterruptedTaskException, StorageException, FileChannelException;

    int getIndexId();

    LockableIterator<LeafTreeNode.KeyValue<K, V>> getSortedIterator() throws StorageException, InterruptedTaskException,
                                                                             FileChannelException;

    K getLastIndex() throws StorageException, InterruptedTaskException, FileChannelException;
}
