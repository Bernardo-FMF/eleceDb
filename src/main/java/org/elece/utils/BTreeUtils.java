package org.elece.utils;

import org.elece.exception.*;
import org.elece.memory.Pointer;
import org.elece.memory.data.BinaryObjectFactory;
import org.elece.memory.tree.node.*;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.session.Session;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class BTreeUtils {
    /**
     * Utility function to calculate how many bytes are required to store a b+ tree node.
     * Also ensures that there is alignment to an 8 byte boundary.
     * The formula reserves 1 byte for the node type flag, calculates the total size of keys and values stored in the node,
     * and then accounts for the child pointers.
     */
    public static int calculateBPlusTreeSize(int degree, int keySize, int valueSize) {
        int value = 1 + (degree * (keySize + valueSize)) + (2 * Pointer.BYTES);
        int remainder = value % Byte.SIZE;
        if (remainder == 0) {
            return value;
        }
        return value + Byte.SIZE - remainder;
    }

    /**
     * @param path       This is the list where we store the sequence of nodes that start from the root to the target leaf node. The leaf node is the node where the new key should be inserted.
     * @param node       This is the root of the tree.
     * @param identifier The new key being added.
     * @param degree     The degree of the btree.
     */
    public static <K extends Comparable<K>> void getPathToResponsibleNode(Session<K> session,
                                                                          List<AbstractTreeNode<K>> path,
                                                                          AbstractTreeNode<K> node, K identifier,
                                                                          int degree) throws
                                                                                      StorageException,
                                                                                      InterruptedTaskException,
                                                                                      FileChannelException {
        // Corresponds to the fast path, if the node being observed is a leaf then we reached the end of the search.
        if (node.getType() == NodeType.LEAF) {
            path.addFirst(node);
            return;
        }

        // We can guarantee from this point forward that the node is in internal node.
        // So we obtain the node pointers to its children.
        List<InternalTreeNode.ChildPointers<K>> childPointersList = ((InternalTreeNode<K>) node).getChildPointersList(degree);
        for (int childrenIndex = 0; childrenIndex < childPointersList.size(); childrenIndex++) {
            InternalTreeNode.ChildPointers<K> childPointers = childPointersList.get(childrenIndex);

            // If the current key in the child pointers is greater than the identifier, traverse the left child
            if (childPointers.getKey().compareTo(identifier) > 0 && childPointers.getLeft() != null) {
                path.addFirst(node);
                getPathToResponsibleNode(session, path, session.read(childPointers.getLeft()), identifier, degree);
                return;
            }

            // If this is the last child pointer and no match is found, traverse the right child
            if (childrenIndex == childPointersList.size() - 1 && childPointers.getRight() != null) {
                path.addFirst(node);
                getPathToResponsibleNode(session, path, session.read(childPointers.getRight()), identifier, degree);
                return;
            }
        }
    }

    public static <K extends Comparable<K>, V> LeafTreeNode<K, V> getResponsibleNode(
            IndexStorageManager indexStorageManager, AbstractTreeNode<K> node, K identifier, int index, int degree,
            NodeFactory<K> nodeFactory, BinaryObjectFactory<V> vIndexBinaryObject) throws
                                                                                   BTreeException,
                                                                                   StorageException,
                                                                                   InterruptedTaskException,
                                                                                   FileChannelException {
        if (node.isLeaf()) {
            return (LeafTreeNode<K, V>) node;
        }

        List<Pointer> childrenList = ((InternalTreeNode<K>) node).getChildrenList();
        List<K> keys = node.getKeyList(degree, vIndexBinaryObject.size());
        int keyIndex;
        K keyAtIndex;
        boolean flag = false;
        for (keyIndex = 0; keyIndex < keys.size(); keyIndex++) {
            keyAtIndex = keys.get(keyIndex);
            if (identifier.compareTo(keyAtIndex) < 0) {
                flag = true;
                break;
            }
        }

        try {
            AbstractTreeNode<K> nextNode;
            if (flag) {
                nextNode = nodeFactory.fromNodeData(indexStorageManager.readNode(index, childrenList.get(keyIndex), node.getKeyValueSize()).get());
            } else {
                nextNode = nodeFactory.fromNodeData(indexStorageManager.readNode(index, childrenList.getLast(), node.getKeyValueSize()).get());
            }
            return getResponsibleNode(indexStorageManager, nextNode, identifier, index, degree, nodeFactory, vIndexBinaryObject);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new InterruptedTaskException(DbError.TASK_INTERRUPTED_ERROR, "File IO operation interrupted");
        } catch (ExecutionException exception) {
            throw new BTreeException(DbError.TASK_INTERRUPTED_ERROR, exception.getMessage());
        }

    }
}
