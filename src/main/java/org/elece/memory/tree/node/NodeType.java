package org.elece.memory.tree.node;

import static org.elece.memory.tree.node.AbstractTreeNode.TYPE_INTERNAL_NODE_BIT;
import static org.elece.memory.tree.node.AbstractTreeNode.TYPE_LEAF_NODE_BIT;

public enum NodeType {
    LEAF(TYPE_LEAF_NODE_BIT), INTERNAL(TYPE_INTERNAL_NODE_BIT);

    private final byte flag;

    NodeType(byte flag) {
        this.flag = flag;
    }

    public byte getFlag() {
        return flag;
    }
}
