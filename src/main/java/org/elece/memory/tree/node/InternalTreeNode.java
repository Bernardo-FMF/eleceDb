package org.elece.memory.tree.node;

import org.elece.exception.btree.BTreeException;
import org.elece.memory.Pointer;
import org.elece.memory.tree.node.data.BinaryObject;
import org.elece.memory.tree.node.data.BinaryObjectFactory;
import org.elece.memory.tree.node.data.PointerBinaryObject;
import org.elece.utils.CollectionUtils;
import org.elece.utils.TreeNodeUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This represents an internal node in a B+ tree.
 * Internal nodes store keys and pointers to child nodes.
 * They are responsible for directing searches to the appropriate leaf nodes.
 *
 * @param <K> The type of keys stored in the tree nodes.
 */
public class InternalTreeNode<K extends Comparable<K>> extends AbstractTreeNode<K> {
    /**
     * Constructor for InternalTreeNode.
     *
     * @param data                 The byte array containing the node's data.
     * @param kBinaryObjectFactory Factory for creating binary representations of keys.
     */
    public InternalTreeNode(byte[] data, BinaryObjectFactory<K> kBinaryObjectFactory) {
        super(data, kBinaryObjectFactory);
        setType(NodeType.INTERNAL);
    }

    /**
     * Retrieves an immutable list of the child pointers stored in the node.
     *
     * @param degree The degree of the B+ tree.
     * @return An immutable list of the child pointers stored in the node.
     */
    public List<ChildPointers<K>> getChildPointersList(int degree) {
        return CollectionUtils.immutableList(getChildPointers(degree));
    }

    protected Iterator<ChildPointers<K>> getChildPointers(int degree) {
        return new ChildPointersIterator(this, degree);
    }

    /**
     * Sets the child pointers in the node.
     *
     * @param childPointers A list of ChildPointers<K> to set in the node.
     * @param degree        The degree of the B+ tree.
     * @param cleanRest     If true, cleans the remaining space in the node's data array.
     * @throws BTreeException If an error occurs while setting child pointers.
     */
    public void setChildPointers(List<ChildPointers<K>> childPointers, int degree, boolean cleanRest) throws BTreeException {
        setModified();
        if (cleanRest) {
            // Clears the existing child pointers in the node's data array.
            TreeNodeUtils.cleanChildrenPointers(this, degree, kBinaryObjectFactory.size(), PointerBinaryObject.BYTES);
        }
        int index = 0;
        for (ChildPointers<K> keyPointer : childPointers) {
            keyPointer.setIndex(index);
            TreeNodeUtils.setKeyAtIndex(this, keyPointer.index, kBinaryObjectFactory.create(keyPointer.key), PointerBinaryObject.BYTES);

            if (index == 0) {
                // For the first key, set both left and right child pointers.
                TreeNodeUtils.setPointerToChild(this, 0, keyPointer.left, kBinaryObjectFactory.size());
                TreeNodeUtils.setPointerToChild(this, 1, keyPointer.right, kBinaryObjectFactory.size());
            } else {
                // For subsequent keys, only set the right child pointer.
                TreeNodeUtils.setPointerToChild(this, keyPointer.index + 1, keyPointer.right, kBinaryObjectFactory.size());
            }
            index++;
        }
    }

    /**
     * Retrieves an immutable list of the child pointers (only pointers, not keys).
     *
     * @return An immutable list of Pointers representing the child nodes.
     */
    public List<Pointer> getChildrenList() {
        return CollectionUtils.immutableList(getChildren());
    }

    /**
     * Retrieves an immutable list of keys stored in the node.
     *
     * @param degree The degree of the B+ tree.
     * @return An immutable list of keys of type K.
     */
    public List<K> getKeyList(int degree) {
        return CollectionUtils.immutableList(getKeys(degree));
    }

    protected Iterator<K> getKeys(int degree) {
        return super.getKeys(degree, Pointer.BYTES);
    }

    protected Iterator<Pointer> getChildren() {
        return new ChildrenIterator(this);
    }

    /**
     * Adds child pointers to the node at the correct position.
     *
     * @param key                The key associated with the child pointers.
     * @param left               The pointer to the left child node.
     * @param right              The pointer to the right child node.
     * @param degree             The degree of the B+ tree.
     * @param clearIfNullPointer If true, removes child pointers if left or right is null.
     * @throws BTreeException If an error occurs while adding child pointers.
     */
    public void addChildPointers(K key, Pointer left, Pointer right, int degree, boolean clearIfNullPointer) throws BTreeException {
        setModified();

        // Add the key to the node and get the index where it was inserted.
        int index = this.addKey(key, degree);

        if (left != null) {
            // Associates the left child pointer with the key.
            // Right child pointer at index corresponds to keys less than the inserted key.
            TreeNodeUtils.setPointerToChild(this, index, left, kBinaryObjectFactory.size());
        } else if (clearIfNullPointer) {
            // Ensures that there are no stale pointers in the case of when the left child pointer is null.
            TreeNodeUtils.removeChildAtIndex(this, index, kBinaryObjectFactory.size());
        }

        if (right != null) {
            // Associates the right child pointer with the key.
            // Right child pointer at index + 1 corresponds to keys greater than or equal to the inserted key.
            TreeNodeUtils.setPointerToChild(this, index + 1, right, kBinaryObjectFactory.size());
        } else if (clearIfNullPointer) {
            // Ensures that there are no stale pointers in the case of when the left child pointer is null.
            TreeNodeUtils.removeChildAtIndex(this, index + 1, kBinaryObjectFactory.size());
        }
    }

    /**
     * Adds a key to the internal node while maintaining sorted order.
     *
     * @param key    The key to be added.
     * @param degree The degree (order) of the B+ tree.
     * @return The index at which the key was inserted.
     * @throws BTreeException If an error occurs during insertion.
     */
    public int addKey(K key, int degree) throws BTreeException {
        // Retrieve the current list of keys from the node.
        List<K> keyList = new ArrayList<>(this.getKeyList(degree));
        int index = CollectionUtils.findIndex(keyList, key);
        // Insert the new key into the keyList at the calculated index.
        // Existing keys at and after this index are shifted to the right.
        keyList.add(index, key);

        // Update the node's data array with the new keys starting from the insertion index.
        // Essentially shifts the keys starting from the newly inserted key index by one.
        for (int keyListIndex = index; keyListIndex < keyList.size() && keyListIndex < degree - 1; keyListIndex++) {
            TreeNodeUtils.setKeyAtIndex(this, keyListIndex, kBinaryObjectFactory.create(keyList.get(keyListIndex)), PointerBinaryObject.BYTES);
        }

        return index;
    }

    /**
     * Inserts a child pointer into the internal node at the specified index.
     *
     * @param index   The index at which to insert the new child pointer.
     * @param pointer The child pointer to be inserted.
     */
    public void addChildAtIndex(int index, Pointer pointer) {
        List<Pointer> childrenList = new ArrayList<>(this.getChildrenList());
        childrenList.add(index, pointer);

        // Update the node's data array with the new keys starting from the insertion index.
        // Essentially shifts the keys starting from the newly inserted key index by one.
        for (int tempIndex = index; tempIndex < childrenList.size(); tempIndex++) {
            TreeNodeUtils.setPointerToChild(this, tempIndex, pointer, kBinaryObjectFactory.size());
        }
    }

    /**
     * Adds a key and its associated child pointer to the internal node,
     * splitting the node if necessary.
     * Returns the child pointers that should be used to create a new node after the split.
     *
     * @param key     The key to be added.
     * @param pointer The child pointer associated with the key.
     * @param degree  The degree (order) of the B+ tree.
     * @return A list of ChildPointers<K> that should be moved to the new node.
     * @throws BTreeException If an error occurs during the operation.
     */
    public List<ChildPointers<K>> addAndSplit(K key, Pointer pointer, int degree) throws BTreeException {
        setModified();
        int mid = (degree - 1) / 2;

        List<K> keyList = new ArrayList<>(getKeyList(degree));
        int index = CollectionUtils.findIndex(keyList, key);
        keyList.add(index, key);

        List<ChildPointers<K>> childPointersList = new ArrayList<>(getChildPointersList(degree));

        // Create a new ChildPointers object for the new key and insert it into the list.
        ChildPointers<K> kChildPointers = childPointersList.get(index == 0 ? 0 : index - 1);
        childPointersList.add(index, new ChildPointers<>(0, key, kChildPointers.getRight(), pointer));

        // If there is a child pointer following the inserted one, update its left pointer.
        if (index + 1 < childPointersList.size()) {
            // Set the left pointer of the next child pointer to the new pointer.
            childPointersList.get(index + 1).setLeft(pointer);
        }

        // Split the childPointersList into two parts: the ones to keep in the current node and the ones to move to a new node.
        List<ChildPointers<K>> toKeep = childPointersList.subList(0, mid + 1);
        // Update the current node's child pointers with the first part.
        this.setChildPointers(toKeep, degree, true);

        // Return the second part of the child pointers, which will be used to create a new node.
        return childPointersList.subList(mid + 1, keyList.size());
    }

    /**
     * Inner class that iterates over child pointers in the node.
     */
    private static class ChildrenIterator implements Iterator<Pointer> {
        private final InternalTreeNode<?> node;
        private int cursor = 0;

        /**
         * Constructor for ChildrenIterator.
         *
         * @param node The InternalTreeNode whose child pointers are to be iterated over.
         */
        private ChildrenIterator(InternalTreeNode<?> node) {
            this.node = node;
        }

        @Override
        public boolean hasNext() {
            return TreeNodeUtils.hasChildPointerAtIndex(this.node, cursor, node.kBinaryObjectFactory.size());
        }

        @Override
        public Pointer next() {
            Pointer pointer = TreeNodeUtils.getChildPointerAtIndex(this.node, cursor, node.kBinaryObjectFactory.size());
            cursor++;
            return pointer;
        }
    }

    /**
     * Inner class that iterates over child pointers and keys in the node.
     */
    private class ChildPointersIterator implements Iterator<ChildPointers<K>> {
        private int cursor = 0;
        private Pointer lastRightPointer;
        private final InternalTreeNode<K> node;
        private final int degree;

        /**
         * Constructor for ChildPointersIterator.
         *
         * @param node   The InternalTreeNode whose child pointers are to be iterated over.
         * @param degree The degree of the B+ tree.
         */
        private ChildPointersIterator(InternalTreeNode<K> node, int degree) {
            this.node = node;
            this.degree = degree;
        }

        @Override
        public boolean hasNext() {
            return TreeNodeUtils.hasKeyAtIndex(node, cursor, degree, kBinaryObjectFactory, PointerBinaryObject.BYTES);
        }

        @Override
        public ChildPointers<K> next() {
            // Retrieves the key at the current cursor position.
            BinaryObject<K> binaryObject = TreeNodeUtils.getKeyAtIndex(node, cursor, kBinaryObjectFactory, PointerBinaryObject.BYTES);
            ChildPointers<K> childPointers;
            if (cursor == 0) {
                // For the first key, retrieve both left and right child pointers.
                childPointers = new ChildPointers<>(cursor, binaryObject.asObject(), TreeNodeUtils.getChildPointerAtIndex(node, 0, kBinaryObjectFactory.size()), TreeNodeUtils.getChildPointerAtIndex(node, 1, kBinaryObjectFactory.size()));
            } else {
                // For subsequent keys, use the last right pointer as the left pointer.
                childPointers = new ChildPointers<>(cursor, binaryObject.asObject(), lastRightPointer, TreeNodeUtils.getChildPointerAtIndex(node, cursor + 1, kBinaryObjectFactory.size()));
            }
            lastRightPointer = childPointers.getRight();

            cursor++;
            return childPointers;
        }
    }

    /**
     * Static inner class representing a key and its associated left and right child pointers.
     *
     * @param <E> The type of the key.
     */
    public static class ChildPointers<E> {
        private int index;
        private final E key;
        private Pointer left;
        private Pointer right;

        /**
         * Constructor for ChildPointers.
         *
         * @param index The index of this entry.
         * @param key   The key associated with the child pointers.
         * @param left  The pointer to the left child node.
         * @param right The pointer to the right child node.
         */
        public ChildPointers(int index, E key, Pointer left, Pointer right) {
            this.index = index;
            this.key = key;
            this.left = left;
            this.right = right;
        }

        public Pointer getLeft() {
            return left;
        }

        public Pointer getRight() {
            return right;
        }

        public E getKey() {
            return key;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public void setLeft(Pointer left) {
            this.left = left;
        }

        public void setRight(Pointer right) {
            this.right = right;
        }
    }
}
