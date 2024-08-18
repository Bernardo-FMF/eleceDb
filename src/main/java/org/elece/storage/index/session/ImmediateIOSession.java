package org.elece.storage.index.session;

import org.elece.memory.KvSize;
import org.elece.memory.Pointer;
import org.elece.memory.tree.node.AbstractTreeNode;
import org.elece.memory.tree.node.INodeFactory;
import org.elece.storage.error.StorageException;
import org.elece.storage.error.type.InternalStorageError;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.NodeData;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * This implementation follows an immediate commit-to-disk approach, where the tree nodes are immediately persisted on disk.
 * This means that a rollback of operations is not possible.
 */
public class ImmediateIOSession<K> extends AbstractIOSession<K> {
    public ImmediateIOSession(IndexStorageManager indexStorageManager, INodeFactory<K> nodeFactory, int indexId, KvSize kvSize) {
        super(indexStorageManager, nodeFactory, indexId, kvSize);
    }

    @Override
    public Optional<AbstractTreeNode<K>> getRoot() throws StorageException {
        try {
            Optional<NodeData> optional = indexStorageManager.getRoot(indexId, kvSize).get();
            return optional.map(nodeFactory::fromNodeData);
        } catch (ExecutionException | InterruptedException exception) {
            throw new StorageException(new InternalStorageError(exception.getMessage()));
        }
    }

    @Override
    public NodeData write(AbstractTreeNode<K> node) throws StorageException {
        try {
            return writeNode(node).get();
        } catch (IOException | ExecutionException | InterruptedException exception) {
            throw new StorageException(new InternalStorageError(exception.getMessage()));
        }
    }

    @Override
    public AbstractTreeNode<K> read(Pointer pointer) throws StorageException {
        try {
            return readNode(pointer);
        } catch (ExecutionException | InterruptedException | IOException exception) {
            throw new StorageException(new InternalStorageError(exception.getMessage()));
        }
    }

    @Override
    public void update(AbstractTreeNode<K> node) throws StorageException {
        try {
            updateNode(node);
        } catch (InterruptedException | IOException | ExecutionException exception) {
            throw new StorageException(new InternalStorageError(exception.getMessage()));
        }
    }

    @Override
    public void remove(AbstractTreeNode<K> node) throws StorageException {
        try {
            removeNode(node.getPointer());
        } catch (ExecutionException | InterruptedException exception) {
            throw new StorageException(new InternalStorageError(exception.getMessage()));
        }
    }

    @Override
    public void commit() throws StorageException {
        // Commit is already implied in the crud operations
    }

    @Override
    public void rollback() throws IOException, InterruptedException, ExecutionException {
        // Rollback is not supported in this implementation, since the commit is not reversible
    }
}
