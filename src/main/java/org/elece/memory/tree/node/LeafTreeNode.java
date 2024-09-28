package org.elece.memory.tree.node;

import org.elece.exception.btree.BTreeException;
import org.elece.memory.Pointer;
import org.elece.memory.tree.node.data.BinaryObjectFactory;
import org.elece.utils.CollectionUtils;
import org.elece.utils.TreeNodeUtils;

import java.util.*;

/**
 * This represents a leaf node in a B+ tree.
 * It stores key-value pairs and pointers to sibling nodes for traversal.
 *
 * @param <K> The type of keys stored in the node.
 * @param <V> The type of values associated with the keys.
 */
public class LeafTreeNode<K extends Comparable<K>, V> extends AbstractTreeNode<K> {
    protected final BinaryObjectFactory<V> vBinaryObjectFactory;

    /**
     * Constructor for LeafTreeNode.
     *
     * @param data                 The byte array representing the node's data.
     * @param kBinaryObjectFactory Factory to create binary objects for keys.
     * @param vBinaryObjectFactory Factory to create binary objects for values.
     */
    public LeafTreeNode(byte[] data, BinaryObjectFactory<K> kBinaryObjectFactory, BinaryObjectFactory<V> vBinaryObjectFactory) {
        super(data, kBinaryObjectFactory);
        this.vBinaryObjectFactory = vBinaryObjectFactory;
        setType(NodeType.LEAF);
    }

    /**
     * Adds a key-value pair to the leaf node and returns the index where it was inserted.
     *
     * @param key    The key to be added.
     * @param value  The value to be associated with the key.
     * @param degree The degree of the B+ tree.
     * @return The index at which the key-value pair was inserted.
     * @throws BTreeException If an error occurs during insertion.
     */
    public int addKeyValue(K key, V value, int degree) throws BTreeException {
        return TreeNodeUtils.addKeyValueAndGetIndex(this, degree, kBinaryObjectFactory, key, vBinaryObjectFactory, value);
    }

    /**
     * Adds a key-value pair and splits the node if necessary.
     * Returns the key-value pairs that should be moved to a new node after the split.
     *
     * @param key    The key to be added.
     * @param value  The value to be associated with the key.
     * @param degree The degree of the B+ tree.
     * @return A list of key-value pairs to be moved to the new node.
     * @throws BTreeException If an error occurs during insertion or splitting.
     */
    public List<KeyValue<K, V>> addAndSplit(K key, V value, int degree) throws BTreeException {
        int mid = (degree - 1) / 2;

        // Get the current list of key-value pairs.
        List<KeyValue<K, V>> allKeyValues = new ArrayList<>(getKeyValueList(degree));
        KeyValue<K, V> keyValue = new KeyValue<>(key, value);

        // Find the index where the new key-value pair should be inserted.
        int index = CollectionUtils.findIndex(allKeyValues, keyValue);

        // Insert the new key-value pair at the calculated index.
        allKeyValues.add(index, keyValue);

        // Split the list into two parts: the first part remains in the current node,
        // and the second part will be moved to a new node.
        List<KeyValue<K, V>> toKeep = allKeyValues.subList(0, mid + 1);
        // Update the current node with the first part.
        this.setKeyValues(toKeep, degree);

        // Return the second part, which will be used to create a new node.
        return allKeyValues.subList(mid + 1, allKeyValues.size());
    }

    public void setKeyValue(int index, KeyValue<K, V> keyValue) throws BTreeException {
        TreeNodeUtils.setKeyValueAtIndex(this, index, kBinaryObjectFactory.create(keyValue.key()), vBinaryObjectFactory.create(keyValue.value()));
    }

    /**
     * Sets the key-value pairs in the node and updates the node's data array.
     *
     * @param keyValueList The list of key-value pairs to set in the node.
     * @param degree       The degree of the B+ tree.
     * @throws BTreeException If an error occurs during the update.
     */
    public void setKeyValues(List<KeyValue<K, V>> keyValueList, int degree) throws BTreeException {
        setModified();

        // Iterate over the key-value list and set each key-value pair in the node.
        for (int index = 0; index < keyValueList.size(); index++) {
            KeyValue<K, V> keyValue = keyValueList.get(index);
            setKeyValue(index, keyValue);
        }

        // Removes possible stale key-value pairs.
        for (int index = keyValueList.size(); index < (degree - 1); index++) {
            TreeNodeUtils.removeKeyValueAtIndex(this, index, kBinaryObjectFactory.size(), vBinaryObjectFactory.size());
        }
    }

    /**
     * Retrieves the pointer to the next sibling leaf node, if it exists.
     *
     * @param degree The degree of the B+ tree.
     * @return An Optional containing the pointer to the next sibling, or empty if none exists.
     */
    public Optional<Pointer> getNextSiblingPointer(int degree) {
        return TreeNodeUtils.getNextPointer(this, degree, kBinaryObjectFactory.size(), vBinaryObjectFactory.size());
    }

    /**
     * Sets the pointer to the next sibling leaf node.
     *
     * @param pointer The pointer to the next sibling node.
     * @param degree  The degree of the B+ tree.
     */
    public void setNextSiblingPointer(Pointer pointer, int degree) {
        setModified();
        TreeNodeUtils.setNextPointer(this, degree, pointer, kBinaryObjectFactory.size(), vBinaryObjectFactory.size());
    }

    /**
     * Sets the pointer to the previous sibling leaf node.
     *
     * @param pointer The pointer to the previous sibling node.
     * @param degree  The degree of the B+ tree.
     */
    public void setPreviousSiblingPointer(Pointer pointer, int degree) {
        setModified();
        TreeNodeUtils.setPreviousPointer(this, degree, pointer, kBinaryObjectFactory.size(), vBinaryObjectFactory.size());
    }

    public List<KeyValue<K, V>> getKeyValueList(int degree) {
        return CollectionUtils.immutableList(getKeyValues(degree));
    }

    protected Iterator<KeyValue<K, V>> getKeyValues(int degree) {
        return new KeyValueIterator(this, degree);
    }

    /**
     * Removes the key-value pair with the specified key from the leaf node.
     *
     * @param key    The key of the key-value pair to remove.
     * @param degree The degree of the B+ tree.
     * @return True if the key-value pair was removed; false otherwise.
     * @throws BTreeException If an error occurs during the operation.
     */
    public boolean removeKeyValue(K key, int degree) throws BTreeException {
        List<KeyValue<K, V>> keyValueList = new ArrayList<>(this.getKeyValueList(degree));

        // Remove the key-value pair matching the specified key.
        boolean removed = keyValueList.removeIf(keyValue -> keyValue.key.compareTo(key) == 0);

        // Update the node's key-values with the modified list.
        setKeyValues(keyValueList, degree);
        return removed;
    }

    /**
     * Retrieves the list of keys stored in the leaf node.
     *
     * @param degree The degree of the B+ tree.
     * @return A list of keys in the leaf node.
     */
    public List<K> getKeyList(int degree) {
        return super.getKeyList(degree, vBinaryObjectFactory.size());
    }

    /**
     * Adds a key-value pair to the leaf node using a KeyValue object.
     *
     * @param keyValue The key-value pair to add.
     * @param degree   The degree of the B+ tree.
     * @return The index at which the key-value pair was inserted.
     * @throws BTreeException If an error occurs during insertion.
     */
    public int addKeyValue(KeyValue<K, V> keyValue, int degree) throws BTreeException {
        return this.addKeyValue(keyValue.key, keyValue.value, degree);
    }

    /**
     * Retrieves the pointer to the previous sibling leaf node, if it exists.
     *
     * @param degree The degree of the B+ tree.
     * @return An Optional containing the previous sibling pointer if it exists; otherwise, Optional.empty().
     */
    public Optional<Pointer> getPreviousSiblingPointer(int degree) {
        return TreeNodeUtils.getPreviousPointer(this, degree, kBinaryObjectFactory.size(), vBinaryObjectFactory.size());
    }

    /**
     * This represents a key-value pair.
     *
     * @param <K> The type of keys, which must be comparable.
     * @param <V> The type of values.
     */
    public record KeyValue<K extends Comparable<K>, V>(K key, V value) implements Comparable<KeyValue<K, V>> {
        @Override
        public int compareTo(KeyValue<K, V> o) {
            return this.key.compareTo(o.key);
        }
    }

    /**
     * An iterator over the key-value pairs stored in the leaf node.
     */
    private class KeyValueIterator implements Iterator<KeyValue<K, V>> {
        private int cursor = 0;

        private final LeafTreeNode<K, V> node;
        private final int degree;

        /**
         * Constructor for KeyValueIterator that corresponds to the given node.
         *
         * @param node   The leaf node to iterate over.
         * @param degree The degree of the B+ tree.
         */
        public KeyValueIterator(LeafTreeNode<K, V> node, int degree) {
            this.node = node;
            this.degree = degree;
        }

        @Override
        public boolean hasNext() {
            return TreeNodeUtils.hasKeyAtIndex(node, cursor, degree, kBinaryObjectFactory, vBinaryObjectFactory.size());
        }

        @Override
        public KeyValue<K, V> next() {
            Map.Entry<K, V> kvAtIndex = TreeNodeUtils.getKeyValueAtIndex(node, cursor, kBinaryObjectFactory, vBinaryObjectFactory);
            cursor++;
            return new KeyValue<>(kvAtIndex.getKey(), kvAtIndex.getValue());
        }
    }
}
