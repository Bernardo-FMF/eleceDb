package org.elece.storage.index.session;

import org.elece.exception.DbError;
import org.elece.exception.FileChannelException;
import org.elece.exception.InterruptedTaskException;
import org.elece.exception.StorageException;
import org.elece.memory.KeyValueSize;
import org.elece.memory.Pointer;
import org.elece.memory.tree.node.AbstractTreeNode;
import org.elece.memory.tree.node.NodeFactory;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.NodeData;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * This implementation follows an immediate commit-to-disk approach, where the tree nodes are immediately persisted on disk.
 * This means that a rollback of operations is not possible.
 */
public class ImmediateSession<K extends Comparable<K>> extends AbstractSession<K> {
    public ImmediateSession(IndexStorageManager indexStorageManager, NodeFactory<K> nodeFactory, int indexId,
                            KeyValueSize keyValueSize) {
        super(indexStorageManager, nodeFactory, indexId, keyValueSize);
    }

    @Override
    public Optional<AbstractTreeNode<K>> getRoot() throws StorageException, FileChannelException,
                                                          InterruptedTaskException {
        Optional<NodeData> optional = handleFuture(indexStorageManager.getRoot(indexId, keyValueSize));
        return optional.map(nodeFactory::fromNodeData);
    }

    @Override
    public NodeData write(AbstractTreeNode<K> node) throws StorageException, InterruptedTaskException,
                                                           FileChannelException {
        try {
            return writeNodeAsync(node).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new InterruptedTaskException(DbError.TASK_INTERRUPTED_ERROR, exception.getMessage());
        } catch (ExecutionException exception) {
            throw new StorageException(DbError.INTERNAL_STORAGE_ERROR, exception.getMessage());
        }
    }

    @Override
    public AbstractTreeNode<K> read(Pointer pointer) throws StorageException, InterruptedTaskException,
                                                            FileChannelException {
        return readNode(pointer);
    }

    @Override
    public void update(AbstractTreeNode<K> node) throws StorageException, InterruptedTaskException,
                                                        FileChannelException {
        updateNode(node);
    }

    @Override
    public void remove(AbstractTreeNode<K> node) throws StorageException, InterruptedTaskException,
                                                        FileChannelException {
        removeNode(node.getPointer());
    }

    @Override
    public void commit() {
        // Commit is already implied in the crud operations
    }

    @Override
    public void rollback() {
        // Rollback is not supported in this implementation, since the commit is not reversible
    }
}
