package org.elece.storage.index.session;

import org.elece.memory.KvSize;
import org.elece.memory.Pointer;
import org.elece.memory.tree.node.AbstractTreeNode;
import org.elece.memory.tree.node.INodeFactory;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.NodeData;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public abstract class AbstractIOSession<K> implements IAtomicIOSession<K> {
    protected final IndexStorageManager indexStorageManager;
    protected final INodeFactory<K> nodeFactory;
    protected final int indexId;
    protected final KvSize kvSize;

    public AbstractIOSession(IndexStorageManager indexStorageManager, INodeFactory<K> nodeFactory, int indexId, KvSize kvSize) {
        this.indexStorageManager = indexStorageManager;
        this.nodeFactory = nodeFactory;
        this.indexId = indexId;
        this.kvSize = kvSize;
    }

    public CompletableFuture<NodeData> writeNode(AbstractTreeNode<?> node) throws IOException, ExecutionException, InterruptedException {
        NodeData nodeData = new NodeData(node.getPointer(), node.getData());
        if (!node.isModified() && node.getPointer() != null) {
            return CompletableFuture.completedFuture(nodeData);
        }
        CompletableFuture<NodeData> output = new CompletableFuture<>();

        if (node.getPointer() == null) {
            indexStorageManager.writeNewNode(indexId, node.getData(), node.isRoot(), node.getKVSize()).whenComplete((nodeData1, throwable) -> {
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

    public AbstractTreeNode<K> readNode(Pointer pointer) throws ExecutionException, InterruptedException, IOException {
        return nodeFactory.fromNodeData(indexStorageManager.readNode(indexId, pointer, kvSize).get());
    }

    public void updateNode(AbstractTreeNode<K> node) throws InterruptedException, IOException, ExecutionException {
        indexStorageManager.updateNode(indexId, node.getData(), node.getPointer(), node.isRoot()).get();
    }

    public void removeNode(Pointer pointer) throws ExecutionException, InterruptedException {
        indexStorageManager.removeNode(indexId, pointer, kvSize).get();
    }

    @Override
    public IndexStorageManager getIndexStorageManager() {
        return indexStorageManager;
    }
}
