package org.elece.memory.tree.node;

import org.elece.memory.Pointer;
import org.elece.memory.tree.node.data.IBinaryObject;
import org.elece.memory.tree.node.data.PointerBinaryObject;

public class LeafTreeNode<K> extends AbstractTreeNode<K> {
    protected final IBinaryObject<Pointer> binaryObject;

    public LeafTreeNode(byte[] data, IBinaryObject<K> binaryObjectKey) {
        super(data, binaryObjectKey);
        this.binaryObject = new PointerBinaryObject();
        setType(NodeType.LEAF);
    }
}
