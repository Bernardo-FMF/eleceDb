package org.elece.memory.tree.node;

import org.elece.memory.tree.node.data.BinaryObject;

public class InternalTreeNode<K> extends AbstractTreeNode<K> {
    public InternalTreeNode(byte[] data, BinaryObject<K> immutableBinaryObjectWrapper) {
        super(data, immutableBinaryObjectWrapper);
        setType(NodeType.INTERNAL);
    }
}
