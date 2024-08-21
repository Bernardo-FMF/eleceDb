package org.elece.memory.tree.node;

import org.elece.memory.KeyValueSize;
import org.elece.memory.Pointer;
import org.elece.memory.tree.node.data.BinaryObject;
import org.elece.memory.tree.node.data.PointerBinaryObject;

public abstract class AbstractTreeNode<K> {
    public static byte TYPE_LEAF_NODE_BIT = 0x02;
    public static byte TYPE_INTERNAL_NODE_BIT = 0x01;
    public static byte ROOT_BIT = 0x04;

    private Pointer pointer;
    private final byte[] data;
    private boolean modified = false;
    protected final BinaryObject<K> keyIBinaryObject;

    public AbstractTreeNode(byte[] data, BinaryObject<K> keyIBinaryObject) {
        this.data = data;
        this.keyIBinaryObject = keyIBinaryObject;
    }

    public boolean isLeaf() {   //  0 0  &  0 0
        return (data[0] & TYPE_LEAF_NODE_BIT) == TYPE_LEAF_NODE_BIT;
    }

    public void setType(NodeType type) {
        setModified();
        this.data[0] = (byte) (data[0] | type.getFlag());
    }

    protected void setModified() {
        this.modified = true;
    }

    public Pointer getPointer() {
        return pointer;
    }

    public void setPointer(Pointer pointer) {
        this.pointer = pointer;
    }

    public byte[] getData() {
        return data;
    }

    public boolean isModified() {
        return modified;
    }

    public boolean isRoot() {
        return (data[0] & ROOT_BIT) == ROOT_BIT;
    }

    public KeyValueSize getKVSize() {
        return new KeyValueSize(keyIBinaryObject.size(), PointerBinaryObject.BYTES);
    }
}
