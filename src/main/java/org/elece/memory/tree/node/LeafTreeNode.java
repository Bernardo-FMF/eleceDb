package org.elece.memory.tree.node;

import org.elece.memory.Pointer;
import org.elece.memory.tree.node.data.BinaryObject;
import org.elece.memory.tree.node.data.PointerBinaryObject;

public class LeafTreeNode<K> extends AbstractTreeNode<K> {
    protected final BinaryObject<Pointer> binaryObject;

    public LeafTreeNode(byte[] data, BinaryObject<K> binaryObjectKey) {
        super(data, binaryObjectKey);
        this.binaryObject = new PointerBinaryObject();
        setType(NodeType.LEAF);
    }
}
