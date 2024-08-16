package org.elece.memory.tree.node;

import org.elece.memory.tree.node.data.IBinaryObject;

public class InternalTreeNode<K> extends AbstractTreeNode<K> {
    public InternalTreeNode(byte[] data, IBinaryObject<K> immutableBinaryObjectWrapper) {
        super(data, immutableBinaryObjectWrapper);
        setType(NodeType.INTERNAL);
    }
}
