package org.elece.storage.index.session;

import org.elece.exception.DbError;
import org.elece.exception.StorageException;
import org.elece.memory.KeyValueSize;
import org.elece.memory.Pointer;
import org.elece.memory.tree.node.AbstractTreeNode;
import org.elece.memory.tree.node.NodeFactory;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.NodeData;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * This implementation follows an in-memory snapshot approach, where the writes are done immediately on disk, but the updates and removals are stored in-memory,
 * only being persisted on disk followed by a call to {@link #commit()}, where the changes are finally persisted. Also, even though the writes are done on disk,
 * the pointer is still saved in-memory, so it can be reverted at any moment.
 * This allows to have flexibility in terms of reversibility, since in the context of a transaction we may roll back the changes previously done.
 * In terms of read operations, first we check the snapshot to see if pointer is stored in-memory, if so we deal we can use that stored value to
 * determine the return value, otherwise, the pointer needs to be read from disk.
 */
public class CommittableSession<K extends Comparable<K>> extends AbstractSession<K> {
    private final Set<Pointer> updated;
    private final List<Pointer> created;
    private final List<Pointer> deleted;
    private final Map<Pointer, AbstractTreeNode<K>> snapshot;
    private final Map<Pointer, AbstractTreeNode<K>> original;
    private AbstractTreeNode<K> root;

    public CommittableSession(IndexStorageManager indexStorageManager, NodeFactory<K> nodeFactory, int indexId,
                              KeyValueSize keyValueSize) {
        super(indexStorageManager, nodeFactory, indexId, keyValueSize);
        updated = new HashSet<>();
        created = new LinkedList<>();
        deleted = new LinkedList<>();
        snapshot = new HashMap<>();
        original = new HashMap<>();
    }

    @Override
    public Optional<AbstractTreeNode<K>> getRoot() throws StorageException {
        if (root == null) {
            Optional<NodeData> optional;
            try {
                optional = indexStorageManager.getRoot(indexId, keyValueSize).get();
            } catch (InterruptedException | ExecutionException exception) {
                throw new StorageException(DbError.INTERNAL_STORAGE_ERROR, exception.getMessage());
            }
            if (optional.isPresent()) {
                AbstractTreeNode<K> baseClusterTreeNode = nodeFactory.fromNodeData(optional.get());
                this.root = baseClusterTreeNode;
                return Optional.of(baseClusterTreeNode);
            }
        } else {
            return Optional.of(root);
        }

        return Optional.empty();
    }

    @Override
    public NodeData write(AbstractTreeNode<K> node) throws StorageException {
        NodeData nodeData = writeNode(node);
        this.created.add(nodeData.pointer());
        this.snapshot.put(nodeData.pointer(), node);
        if (node.isRoot()) {
            root = node;
        }
        return nodeData;
    }

    @Override
    public AbstractTreeNode<K> read(Pointer pointer) throws StorageException {
        if (deleted.contains(pointer)) {
            return null;
        }

        if (updated.contains(pointer)) {
            return snapshot.get(pointer);
        }

        if (created.contains(pointer)) {
            return snapshot.get(pointer);
        }

        AbstractTreeNode<K> baseClusterTreeNode = readNode(pointer);

        snapshot.put(pointer, baseClusterTreeNode);

        byte[] copy = new byte[baseClusterTreeNode.getData().length];
        System.arraycopy(baseClusterTreeNode.getData(), 0, copy, 0, copy.length);
        original.put(pointer, nodeFactory.fromNodeData(new NodeData(pointer, copy)));
        if (baseClusterTreeNode.isRoot()) {
            this.root = baseClusterTreeNode;
        }
        return baseClusterTreeNode;
    }

    @Override
    public void update(AbstractTreeNode<K> node) throws StorageException {
        snapshot.put(node.getPointer(), node);
        updated.add(node.getPointer());

        if (node.isRoot()) {
            root = node;
        }
    }

    @Override
    public void remove(AbstractTreeNode<K> node) {
        deleted.add(node.getPointer());
    }

    @Override
    public void commit() throws StorageException {
        for (Pointer pointer : deleted) {
            try {
                removeNode(pointer);
            } catch (StorageException exception) {
                try {
                    rollback();
                } catch (StorageException nestedException) {
                    throw new StorageException(DbError.ROLLBACK_FAILED, "Failed to rollback after exception");
                }
            }
        }

        try {
            for (Pointer pointer : updated) {
                updateNode(snapshot.get(pointer));
            }
        } catch (StorageException exception) {
            rollback();
        }
    }

    @Override
    public void rollback() throws StorageException {
        for (Pointer pointer : deleted) {
            this.updateNode(original.get(pointer));
        }

        for (Pointer pointer : updated) {
            AbstractTreeNode<K> baseClusterTreeNode = original.get(pointer);
            this.updateNode(baseClusterTreeNode);
        }

        for (Pointer pointer : created) {
            this.removeNode(pointer);
        }
    }
}
