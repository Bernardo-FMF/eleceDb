package org.elece.utils;

import org.elece.exception.btree.BTreeException;
import org.elece.exception.storage.StorageException;
import org.elece.memory.Pointer;
import org.elece.memory.tree.node.AbstractTreeNode;
import org.elece.memory.tree.node.InternalTreeNode;
import org.elece.memory.tree.node.NodeType;
import org.elece.storage.index.session.AtomicIOSession;

import java.util.List;

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
     * @param path       this is the list where we store the sequence of nodes that start from the root to the target leaf node. The leaf node is the node where the new key should be inserted.
     * @param node       this is the root of the tree.
     * @param identifier the new key being added.
     * @param degree     the degree of the btree.
     */
    public static <K extends Comparable<K>> void getPathToResponsibleNode(AtomicIOSession<K> atomicIOSession, List<AbstractTreeNode<K>> path, AbstractTreeNode<K> node, K identifier, int degree) throws BTreeException, StorageException {
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
                getPathToResponsibleNode(atomicIOSession, path, atomicIOSession.read(childPointers.getLeft()), identifier, degree);
                return;
            }

            // If this is the last child pointer and no match is found, traverse the right child
            if (childrenIndex == childPointersList.size() - 1 && childPointers.getRight() != null) {
                path.addFirst(node);
                getPathToResponsibleNode(atomicIOSession, path, atomicIOSession.read(childPointers.getRight()), identifier, degree);
                return;
            }
        }
    }
}
