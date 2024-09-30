package org.elece.memory.tree.node;

import org.elece.memory.data.BinaryObjectFactory;

import static org.elece.memory.tree.node.AbstractTreeNode.TYPE_LEAF_NODE_BIT;

public class DefaultNodeFactory<K extends Comparable<K>, V> implements NodeFactory<K> {
    private final BinaryObjectFactory<K> kBinaryObjectFactory;
    private final BinaryObjectFactory<V> vBinaryObjectFactory;

    public DefaultNodeFactory(BinaryObjectFactory<K> kBinaryObjectFactory, BinaryObjectFactory<V> vBinaryObjectFactory) {
        this.kBinaryObjectFactory = kBinaryObjectFactory;
        this.vBinaryObjectFactory = vBinaryObjectFactory;
    }

    @Override
    public AbstractTreeNode<K> fromBytes(byte[] bytes) {
        if ((bytes[0] & TYPE_LEAF_NODE_BIT) == TYPE_LEAF_NODE_BIT)
            return new LeafTreeNode<>(bytes, kBinaryObjectFactory, vBinaryObjectFactory);
        return new InternalTreeNode<>(bytes, kBinaryObjectFactory);
    }

    @Override
    public AbstractTreeNode<K> fromBytes(byte[] bytes, NodeType type) {
        if (type.equals(NodeType.LEAF))
            return new LeafTreeNode<>(bytes, kBinaryObjectFactory, vBinaryObjectFactory);
        return new InternalTreeNode<>(bytes, kBinaryObjectFactory);
    }
}
