package org.elece.memory.tree.node;

import org.elece.exception.btree.BTreeException;
import org.elece.exception.serialization.SerializationException;
import org.elece.memory.KeyValueSize;
import org.elece.memory.Pointer;
import org.elece.memory.data.BinaryObject;
import org.elece.memory.data.BinaryObjectFactory;
import org.elece.memory.data.PointerBinaryObject;
import org.elece.utils.CollectionUtils;
import org.elece.utils.TreeNodeUtils;

import java.util.Iterator;
import java.util.List;

/**
 * This is the base class that represents a node in a B+ tree.
 * It can be either a leaf node or an internal node.
 *
 * @param <K> The type of keys stored in the tree nodes.
 */
public abstract class AbstractTreeNode<K extends Comparable<K>> {
    /**
     * 0000 0010 - Indicates a leaf node.
     */
    public static byte TYPE_LEAF_NODE_BIT = 0x02;
    /**
     * 0000 0001 - Indicates an internal node.
     */
    public static byte TYPE_INTERNAL_NODE_BIT = 0x01;
    /**
     * 0000 0100 - Indicates the root node.
     */
    public static byte ROOT_BIT = 0x04;

    /**
     * Pointer to the location of this node.
     */
    private Pointer pointer;
    /**
     * The byte array representing the data stored in the node, including flags, keys, values, and pointers.
     */
    private final byte[] data;
    private boolean modified = false;
    protected final BinaryObjectFactory<K> kBinaryObjectFactory;

    /**
     * Constructor for AbstractTreeNode.
     *
     * @param data                 The byte array containing the node's data.
     * @param kBinaryObjectFactory Factory for creating binary representations of keys.
     */
    public AbstractTreeNode(byte[] data, BinaryObjectFactory<K> kBinaryObjectFactory) {
        this.data = data;
        this.kBinaryObjectFactory = kBinaryObjectFactory;
    }

    public boolean isLeaf() {
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

    public void unsetAsRoot() {
        setModified();
        this.data[0] = (byte) (data[0] & ~ROOT_BIT);
    }

    public void setAsRoot() {
        setModified();
        this.data[0] = (byte) (data[0] | ROOT_BIT);
    }

    public KeyValueSize getKeyValueSize() {
        return new KeyValueSize(kBinaryObjectFactory.size(), PointerBinaryObject.BYTES);
    }

    public NodeType getType() {
        if ((data[0] & TYPE_LEAF_NODE_BIT) == TYPE_LEAF_NODE_BIT)
            return NodeType.LEAF;
        if ((data[0] & TYPE_INTERNAL_NODE_BIT) == TYPE_INTERNAL_NODE_BIT)
            return NodeType.INTERNAL;
        return null;
    }

    /**
     * Retrieves an immutable list of the keys stored in the node.
     *
     * @param degree    The degree of the B+ tree (maximum number of children per node).
     * @param valueSize The size of the values stored in the node (used for calculating offsets).
     * @return An immutable list of keys stored in the node.
     */
    public List<K> getKeyList(int degree, int valueSize) {
        return CollectionUtils.immutableList(getKeys(degree, valueSize));
    }

    protected Iterator<K> getKeys(int degree, int valueSize) {
        return new TreeNodeKeysIterator<K>(this, degree, valueSize);
    }

    /**
     * Sets the key at the specified index in the node.
     *
     * @param index     The index at which to set the key.
     * @param key       The key to set.
     * @param valueSize The size of the value associated with the key.
     * @throws BTreeException If an error occurs during the operation.
     */
    public void setKey(int index, K key, int valueSize) throws BTreeException, SerializationException {
        TreeNodeUtils.setKeyAtIndex(this, index, kBinaryObjectFactory.create(key), valueSize);
    }

    /**
     * Removes the key at the specified index from the node.
     *
     * @param index     The index of the key to remove.
     * @param degree    The degree of the B+ tree.
     * @param valueSize The size of the value associated with the key.
     * @throws BTreeException If an error occurs during the operation.
     */
    public void removeKey(int index, int degree, int valueSize) throws BTreeException, SerializationException {
        List<K> keyList = this.getKeyList(degree, valueSize);

        // Remove the key at the specified index.
        TreeNodeUtils.removeKeyAtIndex(this, index, kBinaryObjectFactory.size(), valueSize);

        // Get the sublist of keys after the removed index.
        List<K> subList = keyList.subList(index + 1, keyList.size());
        int lastIndex = -1;

        // Shift the keys to fill the gap.
        for (int subListIndex = 0; subListIndex < subList.size(); subListIndex++) {
            lastIndex = index + subListIndex;
            TreeNodeUtils.setKeyAtIndex(this, lastIndex, kBinaryObjectFactory.create(subList.get(subListIndex)), valueSize);
        }
        if (lastIndex != -1) {
            // Clear any remaining child pointer slots.
            for (int i = lastIndex + 1; i < degree - 1; i++) {
                TreeNodeUtils.removeKeyAtIndex(this, i, kBinaryObjectFactory.size(), valueSize);
            }
        }
    }

    /**
     * Private inner class that implements Iterator<K> to iterate over the keys in the node.
     *
     * @param <K> The type of keys stored in the node.
     */
    private static class TreeNodeKeysIterator<K extends Comparable<K>> implements Iterator<K> {
        private final AbstractTreeNode<K> node;
        private final int degree;
        private final int valueSize;
        private int cursor;
        private boolean hasNext = true;

        /**
         * Constructor for TreeNodeKeysIterator.
         *
         * @param node      The node whose keys are to be iterated over.
         * @param degree    The degree of the B+ tree.
         * @param valueSize The size of the values stored in the node.
         */
        private TreeNodeKeysIterator(AbstractTreeNode<K> node, int degree, int valueSize) {
            this.node = node;
            this.degree = degree;
            this.valueSize = valueSize;
        }

        /**
         * Check if there is a key at the current cursor index.
         * Updates {@link #hasNext} based on the result, which is used as cache for future calls to this method.
         */
        @Override
        public boolean hasNext() {
            if (!hasNext) {
                return false;
            }

            hasNext = TreeNodeUtils.hasKeyAtIndex(this.node, cursor, degree, this.node.kBinaryObjectFactory, valueSize);
            return hasNext;
        }

        @Override
        public K next() {
            BinaryObject<K> binaryObject = TreeNodeUtils.getKeyAtIndex(this.node, cursor, this.node.kBinaryObjectFactory, valueSize);
            cursor++;
            return binaryObject.asObject();
        }
    }
}
