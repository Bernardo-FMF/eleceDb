package org.elece.storage.index.session;

import org.elece.exception.storage.StorageException;
import org.elece.memory.Pointer;
import org.elece.memory.tree.node.AbstractTreeNode;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.NodeData;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public interface AtomicIOSession<K extends Comparable<K>> {
    Optional<AbstractTreeNode<K>> getRoot() throws StorageException, InterruptedException, ExecutionException;

    NodeData write(AbstractTreeNode<K> node) throws StorageException;

    AbstractTreeNode<K> read(Pointer pointer) throws StorageException;

    void update(AbstractTreeNode<K> node) throws StorageException;

    void remove(AbstractTreeNode<K> node) throws StorageException;

    void commit() throws StorageException;

    void rollback() throws IOException, InterruptedException, ExecutionException, StorageException;

    IndexStorageManager getIndexStorageManager();
}
