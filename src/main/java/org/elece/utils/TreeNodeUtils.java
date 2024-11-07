package org.elece.utils;

import org.elece.exception.BTreeException;
import org.elece.exception.DbError;
import org.elece.exception.SerializationException;
import org.elece.memory.Pointer;
import org.elece.memory.data.BinaryObject;
import org.elece.memory.data.BinaryObjectFactory;
import org.elece.memory.tree.node.AbstractTreeNode;
import org.elece.memory.tree.node.InternalTreeNode;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;

public class TreeNodeUtils {
    private static final int OFFSET_TREE_NODE_FLAGS_END = 1;
    private static final int OFFSET_INTERNAL_NODE_KEY_BEGIN = OFFSET_TREE_NODE_FLAGS_END + Pointer.BYTES;
    private static final int OFFSET_LEAF_NODE_KEY_BEGIN = OFFSET_TREE_NODE_FLAGS_END;
    private static final int SIZE_LEAF_NODE_SIBLING_POINTERS = 2 * Pointer.BYTES;

    public static <K> boolean hasKeyAtIndex(AbstractTreeNode<?> treeNode, int index, int degree,
                                            BinaryObjectFactory<K> kBinaryObjectFactory, int valueSize) {
        // Ensures that the index is within the allowable range, since a node can have at modes degree - 1 keys.
        if (index >= degree - 1) {
            return false;
        }

        // Obtain index to determine where the key should be located within the byte array.
        int keyStartIndex = getKeyStartOffset(treeNode, index, kBinaryObjectFactory.size(), valueSize);
        // Easy corner case to validate, if the obtained index plus the size of the key surpasses the length of the nodes' data.
        if (keyStartIndex + kBinaryObjectFactory.size() > treeNode.getData().length) {
            return false;
        }

        return !BinaryUtils.isAllZeros(treeNode.getData(), keyStartIndex, kBinaryObjectFactory.size()) || !BinaryUtils.isAllZeros(treeNode.getData(), keyStartIndex + kBinaryObjectFactory.size(), valueSize);
    }

    private static int getKeyStartOffset(AbstractTreeNode<?> treeNode, int index, int keySize, int valueSize) {
        // Calculates the starting byte offset for a key at a given index within a node's data array.
        if (!treeNode.isLeaf()) {
            return OFFSET_INTERNAL_NODE_KEY_BEGIN + (index * (keySize + valueSize));
        } else {
            return OFFSET_TREE_NODE_FLAGS_END + (index * (keySize + valueSize));
        }
    }

    public static void cleanChildrenPointers(InternalTreeNode<?> node, int degree, int keySize, int valueSize) {
        // This accounts to the maximum amount of keys an internal node can have (degree - 1).
        int len = ((degree - 1) * ((keySize + valueSize)) + Pointer.BYTES);
        // Overwrite the specified portion of the node's data array with zeros.
        // Note that the overwritten section is after the node's flag that determines its type.
        System.arraycopy(new byte[len], 0, node.getData(), OFFSET_TREE_NODE_FLAGS_END, len);
    }

    public static void setPointerToChild(AbstractTreeNode<?> node, int index, Pointer pointer, int keySize) {
        if (index == 0) {
            // For the first child pointer (index = 0), the pointer is stored immediately after the node's flags.
            System.arraycopy(pointer.toBytes(), 0, node.getData(), OFFSET_TREE_NODE_FLAGS_END, Pointer.BYTES);
        } else {
            // For subsequent child pointers (index > 0), the pointer is stored after the corresponding key.
            System.arraycopy(pointer.toBytes(), 0, node.getData(), calculateDataArrayPosition(index, keySize), Pointer.BYTES);
        }
    }

    public static <K extends Comparable<K>> void setKeyAtIndex(AbstractTreeNode<?> node, int index,
                                                               BinaryObject<K> binaryObject, int valueSize) {
        // Calculate the byte offset within the node's data array where the key should be stored.
        int keyStartIndex = getKeyStartOffset(node, index, binaryObject.size(), valueSize);
        System.arraycopy(binaryObject.getBytes(), 0, node.getData(), keyStartIndex, binaryObject.size());
    }

    public static <K extends Comparable<K>, N extends Comparable<N>> BinaryObject<K> getKeyAtIndex(
            AbstractTreeNode<N> node, int index, BinaryObjectFactory<K> kBinaryObjectFactory, int valueSize) {
        // Calculate the byte offset within the node's data array where the key is stored.
        int keyStartIndex = getKeyStartOffset(node, index, kBinaryObjectFactory.size(), valueSize);

        // Creates the binary object from the node's data, present in the obtained index.
        return kBinaryObjectFactory.create(node.getData(), keyStartIndex);
    }

    public static Pointer getChildPointerAtIndex(AbstractTreeNode<?> node, int index, int keySize) {
        // The position of the pointer within the node's data is calculated by taking into account the flag byte,
        // and then we skip the total size of the pointer and key size times the index.
        return Pointer.fromBytes(node.getData(), calculateDataArrayPosition(index, keySize));
    }

    public static boolean hasChildPointerAtIndex(AbstractTreeNode<?> node, int index, int keySize) {
        // The index calculated cannot surpass the data array length.
        int dataIndex = calculateDataArrayPosition(index, keySize);
        if (dataIndex > node.getData().length) {
            return false;
        }

        return node.getData()[dataIndex] == Pointer.TYPE_NODE;
    }

    public static <K extends Comparable<K>, V> int addKeyValueAndGetIndex(AbstractTreeNode<?> node, int degree,
                                                                          BinaryObjectFactory<K> kBinaryObjectFactory,
                                                                          K key,
                                                                          BinaryObjectFactory<V> vBinaryObjectFactory,
                                                                          V value) throws BTreeException,
                                                                                          SerializationException {
        int indexToFill = -1;
        BinaryObject<K> keyAtIndex;
        int keySize = kBinaryObjectFactory.size();
        int valueSize = vBinaryObjectFactory.size();

        int maxNodeSize = degree - 1;

        for (int nodeIndex = 0; nodeIndex < maxNodeSize; nodeIndex++) {
            if (!hasKeyAtIndex(node, nodeIndex, degree, kBinaryObjectFactory, valueSize)) {
                indexToFill = nodeIndex;
                break;
            }

            // Retrieve the key at the current index in the node.
            keyAtIndex = getKeyAtIndex(node, nodeIndex, kBinaryObjectFactory, valueSize);

            // Deserialize the key.
            K data = keyAtIndex.asObject();

            // Check if the slot is empty or if the existing key is greater than the new key.
            // Comparing the keys allows to maintain the keys in the node ordered.
            if (data.compareTo(key) > 0) {
                indexToFill = nodeIndex;
                break;
            }
        }

        if (indexToFill == -1) {
            throw new BTreeException(DbError.FAILED_TO_FIND_INDEX_IN_NODE_ERROR, "Failed to find index in node to insert key");
        }

        int bufferSize = ((maxNodeSize - indexToFill - 1) * (keySize + valueSize));

        // Copy the existing key-value pairs that will be shifted into the temporary buffer.
        byte[] temp = new byte[bufferSize];
        System.arraycopy(node.getData(), OFFSET_LEAF_NODE_KEY_BEGIN + (indexToFill * (keySize + valueSize)), temp, 0, temp.length);

        // Insert the new key-value pair at the determined index.
        setKeyValueAtIndex(node, indexToFill, kBinaryObjectFactory.create(key), vBinaryObjectFactory.create(value));

        // Shift the existing key-value pairs back into the node's data array after the new key-value pair.
        System.arraycopy(temp, 0, node.getData(), OFFSET_LEAF_NODE_KEY_BEGIN + ((indexToFill + 1) * (keySize + valueSize)), temp.length);

        return indexToFill;
    }

    public static <K extends Comparable<K>, V> Map.Entry<K, V> getKeyValueAtIndex(AbstractTreeNode<K> node, int index,
                                                                                  BinaryObjectFactory<K> kBinaryObjectFactory,
                                                                                  BinaryObjectFactory<V> vBinaryObjectFactory) {
        int keyStartIndex = getKeyStartOffset(node, index, kBinaryObjectFactory.size(), vBinaryObjectFactory.size());
        return new AbstractMap.SimpleImmutableEntry<>(
                kBinaryObjectFactory.create(node.getData(), keyStartIndex).asObject(),
                vBinaryObjectFactory.create(node.getData(), keyStartIndex + kBinaryObjectFactory.size()).asObject()
        );
    }

    public static void removeKeyValueAtIndex(AbstractTreeNode<?> node, int index, int keySize, int valueSize) {
        // Calculate the offset where the next key-value pair starts.
        // This helps determine whether there are more key-value pairs after the one being removed.
        int nextIndexOffset = getKeyStartOffset(node, index + 1, keySize, valueSize);
        // Determine the position in the data array where the sibling pointers start.
        int siblingPointersOffset = node.getData().length - SIZE_LEAF_NODE_SIBLING_POINTERS;

        // Check if the next key-value pair starts before the sibling pointers.
        // There are more key-value pairs after the one being removed.
        // We need to shift the subsequent key-value pairs to fill the gap.
        if (nextIndexOffset < siblingPointersOffset) {
            // The key-value pair being removed is the last one before the sibling pointers.
            // No shifting is required; we only need to clear the key-value pair.
            System.arraycopy(new byte[keySize + valueSize], 0, node.getData(), getKeyStartOffset(node, index, keySize, valueSize), keySize + valueSize);
        } else {
            // Shift the subsequent key-value pairs to the left, overwriting the removed key-value pair.
            System.arraycopy(node.getData(), nextIndexOffset, node.getData(), getKeyStartOffset(node, index, keySize, valueSize), nextIndexOffset - SIZE_LEAF_NODE_SIBLING_POINTERS);
            // Clear the now redundant last key-value pair slot (after shifting) by filling it with zeros.
            System.arraycopy(new byte[keySize + valueSize], 0, node.getData(), node.getData().length - SIZE_LEAF_NODE_SIBLING_POINTERS - (keySize + valueSize), keySize + valueSize);
        }
    }

    public static <K extends Comparable<K>, V> void setKeyValueAtIndex(AbstractTreeNode<?> node, int index,
                                                                       BinaryObject<K> kBinaryObject,
                                                                       BinaryObject<V> vBinaryObject) {
        // Copy the serialized key bytes into the node's data array at the calculated offset.
        int keyOffset = OFFSET_LEAF_NODE_KEY_BEGIN + (index * (kBinaryObject.size() + vBinaryObject.size()));
        System.arraycopy(kBinaryObject.getBytes(), 0, node.getData(), keyOffset, kBinaryObject.size());

        // Copy the serialized value bytes into the node's data array at the calculated offset.
        int valueOffset = keyOffset + kBinaryObject.size();
        System.arraycopy(vBinaryObject.getBytes(), 0, node.getData(), valueOffset, vBinaryObject.size());
    }

    public static Optional<Pointer> getNextPointer(AbstractTreeNode<?> node, int degree, int keySize, int valueSize) {
        // Total space allocated for key-value pairs.
        // Even if the node is not full, the calculation accounts for the maximum capacity.
        int position = OFFSET_LEAF_NODE_KEY_BEGIN + ((degree - 1) * (keySize + valueSize)) + Pointer.BYTES;

        // Check if the next pointer is valid.
        // If the first byte at the position is zero, there is no next pointer.
        if (node.getData()[position] == (byte) 0x0) {
            return Optional.empty();
        }

        return Optional.of(Pointer.fromBytes(node.getData(), position));
    }

    public static <K extends Comparable<K>, V> void setNextPointer(AbstractTreeNode<?> node, int degree,
                                                                   Pointer pointer, int keySize, int valueSize) {
        // Calculate the position in the data array where the next pointer should be stored.
        // This is after all key-value pairs and the previous pointer.
        int position = OFFSET_LEAF_NODE_KEY_BEGIN + ((degree - 1) * (keySize + valueSize)) + Pointer.BYTES;
        System.arraycopy(pointer.toBytes(), 0, node.getData(), position, Pointer.BYTES);
    }

    public static <K extends Comparable<K>, V> void setPreviousPointer(AbstractTreeNode<?> node, int degree,
                                                                       Pointer pointer, int keySize, int valueSize) {
        // Calculate the position in the data array where the next pointer should be stored.
        // This is after all key-value pairs.
        int position = OFFSET_LEAF_NODE_KEY_BEGIN + ((degree - 1) * (keySize + valueSize));
        System.arraycopy(pointer.toBytes(), 0, node.getData(), position, Pointer.BYTES);
    }

    public static <K extends Comparable<K>> void removeChildAtIndex(AbstractTreeNode<?> node, int index, int keySize) {
        // Overwrite the child pointer with zeros to remove it.
        System.arraycopy(new byte[Pointer.BYTES], 0, node.getData(), calculateDataArrayPosition(index, keySize), Pointer.BYTES);
    }

    private static int calculateDataArrayPosition(int index, int keySize) {
        return OFFSET_TREE_NODE_FLAGS_END + (index * (Pointer.BYTES + keySize));
    }

    /**
     * Retrieves the pointer to the previous sibling of a leaf node.
     *
     * @param node      The leaf node from which to retrieve the previous sibling pointer.
     * @param degree    The degree (order) of the B+ tree.
     * @param keySize   The size of the key in bytes.
     * @param valueSize The size of the value in bytes.
     * @return An Optional containing the previous sibling pointer if it exists; otherwise, Optional.empty().
     */
    public static Optional<Pointer> getPreviousPointer(AbstractTreeNode<?> node, int degree, int keySize,
                                                       int valueSize) {
        // Calculate the offset where the previous sibling pointer is stored.
        int offset = OFFSET_LEAF_NODE_KEY_BEGIN + ((degree - 1) * (keySize + valueSize));

        // Check if the byte at the offset is zero, indicating no previous sibling pointer.
        if (node.getData()[offset] == (byte) 0x0) {
            return Optional.empty();
        }

        return Optional.of(Pointer.fromBytes(node.getData(), offset));
    }

    /**
     * Removes the key at the specified index from the node by clearing its bytes.
     *
     * @param treeNode  The node from which to remove the key.
     * @param index     The index of the key to remove.
     * @param keySize   The size of the key in bytes.
     * @param valueSize The size of the value in bytes.
     */
    public static void removeKeyAtIndex(AbstractTreeNode<?> treeNode, int index, int keySize, int valueSize) {
        // Calculate the offset where the key starts in the data array.
        int keyOffset = getKeyStartOffset(treeNode, index, keySize, valueSize);

        // Overwrite the key's bytes in the data array with zeros.
        System.arraycopy(new byte[keySize], 0, treeNode.getData(), keyOffset, keySize);
    }
}
