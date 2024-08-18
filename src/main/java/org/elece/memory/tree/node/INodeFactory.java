package org.elece.memory.tree.node;

import org.elece.memory.Pointer;
import org.elece.storage.index.NodeData;

public interface INodeFactory<K> {
    AbstractTreeNode<K> fromBytes(byte[] bytes);

    default AbstractTreeNode<K> fromBytes(byte[] bytes, Pointer pointer) {
        AbstractTreeNode<K> treeNode = this.fromBytes(bytes);
        treeNode.setPointer(pointer);
        return treeNode;
    }

    AbstractTreeNode<K> fromBytes(byte[] emptyNode, NodeType type);

    default AbstractTreeNode<K> fromNodeData(NodeData nodeData) {
        return this.fromBytes(nodeData.bytes(), nodeData.pointer());
    }
}
