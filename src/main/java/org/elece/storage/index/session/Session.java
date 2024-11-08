package org.elece.storage.index.session;

import org.elece.exception.FileChannelException;
import org.elece.exception.InterruptedTaskException;
import org.elece.exception.StorageException;
import org.elece.memory.Pointer;
import org.elece.memory.tree.node.AbstractTreeNode;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.NodeData;

import java.util.Optional;

public interface Session<K extends Comparable<K>> {
    Optional<AbstractTreeNode<K>> getRoot() throws StorageException, FileChannelException, InterruptedTaskException;

    NodeData write(AbstractTreeNode<K> node) throws StorageException, InterruptedTaskException, FileChannelException;

    AbstractTreeNode<K> read(Pointer pointer) throws StorageException, InterruptedTaskException, FileChannelException;

    void update(AbstractTreeNode<K> node) throws StorageException, InterruptedTaskException, FileChannelException;

    void remove(AbstractTreeNode<K> node) throws StorageException, InterruptedTaskException, FileChannelException;

    void commit() throws StorageException, InterruptedTaskException, FileChannelException;

    void rollback() throws StorageException, InterruptedTaskException, FileChannelException;

    IndexStorageManager getIndexStorageManager();
}
