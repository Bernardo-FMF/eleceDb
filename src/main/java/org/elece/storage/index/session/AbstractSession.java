package org.elece.storage.index.session;

import org.elece.exception.StorageException;
import org.elece.memory.KeyValueSize;
import org.elece.memory.Pointer;
import org.elece.memory.tree.node.AbstractTreeNode;
import org.elece.memory.tree.node.NodeFactory;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.NodeData;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public abstract class AbstractSession<K extends Comparable<K>> implements Session<K> {
    protected final IndexStorageManager indexStorageManager;
    protected final NodeFactory<K> nodeFactory;
    protected final int indexId;
    protected final KeyValueSize keyValueSize;

    protected AbstractSession(IndexStorageManager indexStorageManager, NodeFactory<K> nodeFactory, int indexId,
                              KeyValueSize keyValueSize) {
        this.indexStorageManager = indexStorageManager;
        this.nodeFactory = nodeFactory;
        this.indexId = indexId;
        this.keyValueSize = keyValueSize;
    }

    public NodeData writeNode(AbstractTreeNode<?> node) throws StorageException {
        try {
            return writeNodeAsync(node).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<NodeData> writeNodeAsync(AbstractTreeNode<?> node) throws StorageException {
        NodeData nodeData = new NodeData(node.getPointer(), node.getData());
        if (!node.isModified() && node.getPointer() != null) {
            return CompletableFuture.completedFuture(nodeData);
        }
        CompletableFuture<NodeData> output = new CompletableFuture<>();

        if (node.getPointer() == null) {
            indexStorageManager.writeNewNode(indexId, node.getData(), node.isRoot(), node.getKeyValueSize()).whenComplete((nodeData1, throwable) -> {
                if (throwable != null) {
                    output.completeExceptionally(throwable);
                    return;
                }
                node.setPointer(nodeData1.pointer());
                output.complete(nodeData1);
            });
        } else {
            indexStorageManager.updateNode(indexId, node.getData(), node.getPointer(), node.isRoot()).whenComplete((integer, throwable) -> {
                if (throwable != null) {
                    output.completeExceptionally(throwable);
                    return;
                }
                output.complete(nodeData);
            });
        }
        return output;
    }

    public AbstractTreeNode<K> readNode(Pointer pointer) throws StorageException {
        return nodeFactory.fromNodeData(indexStorageManager.readNode(indexId, pointer, keyValueSize).get());
    }

    public void updateNode(AbstractTreeNode<K> node) throws StorageException {
        indexStorageManager.updateNode(indexId, node.getData(), node.getPointer(), node.isRoot()).get();
    }

    public void removeNode(Pointer pointer) throws StorageException {
        indexStorageManager.removeNode(indexId, pointer, keyValueSize).get();
    }

    @Override
    public IndexStorageManager getIndexStorageManager() {
        return indexStorageManager;
    }
}
