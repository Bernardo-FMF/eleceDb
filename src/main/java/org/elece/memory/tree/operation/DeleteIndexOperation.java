package org.elece.memory.tree.operation;

import org.elece.config.DbConfig;
import org.elece.exception.*;
import org.elece.memory.Pointer;
import org.elece.memory.data.BinaryObjectFactory;
import org.elece.memory.tree.node.AbstractTreeNode;
import org.elece.memory.tree.node.InternalTreeNode;
import org.elece.memory.tree.node.LeafTreeNode;
import org.elece.memory.tree.node.NodeFactory;
import org.elece.storage.index.session.Session;
import org.elece.utils.BTreeUtils;

import java.util.*;

/**
 * Handles the deletion of a key-value pair (index) from a tree.
 *
 * @param <K> The type of keys.
 * @param <V> The type of values associated with the keys.
 */
public class DeleteIndexOperation<K extends Comparable<K>, V> {
    private final DbConfig dbConfig;
    private final Session<K> session;
    private final BinaryObjectFactory<V> vBinaryObjectFactory;
    private final NodeFactory<K> nodeFactory;
    private final int indexId;
    private final int minKeys;

    /**
     * Constructor for DeleteIndexOperation.
     *
     * @param dbConfig             The database configuration.
     * @param session              The atomic IO session for storage operations.
     * @param vBinaryObjectFactory Factory to create binary objects for values.
     * @param nodeFactory          Factory to create tree nodes.
     * @param indexId              The identifier for the index.
     */
    public DeleteIndexOperation(DbConfig dbConfig, Session<K> session, BinaryObjectFactory<V> vBinaryObjectFactory,
                                NodeFactory<K> nodeFactory, int indexId) {
        this.dbConfig = dbConfig;
        this.indexId = indexId;
        this.session = session;
        this.minKeys = (dbConfig.getBTreeDegree() - 1) / 2;
        this.vBinaryObjectFactory = vBinaryObjectFactory;
        this.nodeFactory = nodeFactory;
    }

    /**
     * Removes a key-value pair from the tree.
     * In a tree each node (except the root) must have at least a minimum number of keys to maintain the balance.
     * So when a key is deleted, a node can end up as having fewer keys than {@link #minKeys}.
     * In this situation there are two ways to balance the tree again:
     * - borrow a key from a sibling node.
     * - merge the under filled node with a sibling node.
     *
     * @param root       The root node of the tree.
     * @param identifier The key to be deleted.
     * @return True if the key was successfully deleted; false otherwise.
     * @throws BTreeException   If an error occurs during deletion.
     * @throws StorageException If an error occurs during storage operations.
     */
    public boolean removeIndex(AbstractTreeNode<K> root, K identifier) throws BTreeException, StorageException,
                                                                              SerializationException,
                                                                              InterruptedTaskException,
                                                                              FileChannelException {
        int bTreeDegree = dbConfig.getBTreeDegree();

        List<AbstractTreeNode<K>> path = new LinkedList<>();

        // Find the path to the node responsible for the key.
        BTreeUtils.getPathToResponsibleNode(session, path, root, identifier, bTreeDegree);

        boolean removalResult = false;

        // Traverse the path from the leaf to the root.
        for (int index = 0; index < path.size(); index++) {
            AbstractTreeNode<K> currentNode = path.get(index);

            // The first node is a leaf node.
            if (index == 0) {
                LeafTreeNode<K, ?> leafNode = (LeafTreeNode<K, ?>) currentNode;

                // Attempt to remove the key-value pair.
                removalResult = leafNode.removeKeyValue(identifier, bTreeDegree);

                session.update(leafNode);

                // Check if under filled and balance the tree if necessary.
                if (removalResult && !leafNode.isRoot() && leafNode.getKeyList(bTreeDegree).size() < minKeys) {
                    InternalTreeNode<K> parentNode = (InternalTreeNode<K>) path.get(index + 1);

                    // Balance the tree.
                    this.fillNode(leafNode, parentNode, parentNode.getIndexOfChild(currentNode.getPointer()), bTreeDegree);
                }
            } else {
                // Handle the internal nodes.
                this.checkInternalNode((InternalTreeNode<K>) path.get(index), path, index, identifier, bTreeDegree);
            }
        }
        session.commit();

        return removalResult;
    }

    /**
     * Deletes a key from an internal node and ensures the tree remains balanced.
     *
     * @param parent      The parent node.
     * @param node        The internal node from which the key is deleted.
     * @param index       The index of the key to delete.
     * @param bTreeDegree The degree of the tree.
     * @throws BTreeException   If an error occurs during deletion.
     * @throws StorageException If an error occurs during storage operations.
     */
    private void deleteInternalNode(InternalTreeNode<K> parent, InternalTreeNode<K> node, int index,
                                    int bTreeDegree) throws
                                                     BTreeException,
                                                     StorageException,
                                                     SerializationException,
                                                     InterruptedTaskException,
                                                     FileChannelException {
        List<Pointer> childrenList = node.getChildrenList();
        if (index != 0) {
            // Try to replace the key with its predecessor.
            AbstractTreeNode<K> leftIndexNode = session.read(childrenList.get(index - 1));
            if (leftIndexNode.getKeyList(bTreeDegree, vBinaryObjectFactory.size()).size() >= minKeys) {
                K predecessor = this.getPredecessor(node, index, bTreeDegree);
                node.setKey(index, predecessor);
                session.update(node);
            }
        } else {
            // Try to replace the key with its successor.
            AbstractTreeNode<K> rightIDXChild = session.read(childrenList.get(index + 1));
            if (rightIDXChild.getKeyList(bTreeDegree, vBinaryObjectFactory.size()).size() >= minKeys) {
                K successor = getSuccessor(node, index, bTreeDegree);
                node.setKey(index, successor);
                session.update(node);
            } else {
                // Merge nodes if replacement isn't possible.
                merge(parent, node, index, bTreeDegree);
            }
        }
    }

    /**
     * Retrieves the predecessor of a key in the tree.
     *
     * @param node        The internal node.
     * @param index       The index of the key.
     * @param bTreeDegree The degree of the tree.
     * @return The predecessor key.
     * @throws StorageException If an error occurs during storage operations.
     */
    private K getPredecessor(InternalTreeNode<K> node, int index, int bTreeDegree) throws StorageException,
                                                                                          InterruptedTaskException,
                                                                                          FileChannelException {
        // Obtain the current children node.
        AbstractTreeNode<K> current = session.read(node.getChildrenList().get(index));

        // Traverse to the rightmost leaf node.
        while (!current.isLeaf()) {
            current = session.read(node.getChildrenList().get(current.getKeyList(bTreeDegree, vBinaryObjectFactory.size()).size()));
        }

        // Return the last key in the leaf node.
        return current.getKeyList(bTreeDegree, vBinaryObjectFactory.size()).getLast();
    }

    /**
     * Retrieves the successor of a key in the tree.
     *
     * @param node        The internal node.
     * @param index       The index of the key.
     * @param bTreeDegree The degree of the tree.
     * @return The successor key.
     * @throws StorageException If an error occurs during storage operations.
     */
    private K getSuccessor(InternalTreeNode<K> node, int index, int bTreeDegree) throws StorageException,
                                                                                        InterruptedTaskException,
                                                                                        FileChannelException {
        // Obtain the next children node.
        AbstractTreeNode<K> current = session.read(node.getChildrenList().get(index + 1));

        // Traverse to the leftmost leaf node.
        while (!current.isLeaf()) {
            current = session.read(((InternalTreeNode<K>) current).getChildrenList().getFirst());
        }

        // Return the first key in the leaf node.
        return current.getKeyList(bTreeDegree, vBinaryObjectFactory.size()).getFirst();
    }

    /**
     * Checks and handles deletion in an internal node.
     *
     * @param internalTreeNode The internal node being checked.
     * @param path             The path from root to the current node.
     * @param index            The index of the current node in the path.
     * @param identifier       The key being deleted.
     * @param bTreeDegree      The degree of the tree.
     * @throws BTreeException   If an error occurs during deletion.
     * @throws StorageException If an error occurs during storage operations.
     */
    private void checkInternalNode(InternalTreeNode<K> internalTreeNode, List<AbstractTreeNode<K>> path, int index,
                                   K identifier, int bTreeDegree) throws
                                                                  BTreeException,
                                                                  StorageException,
                                                                  SerializationException,
                                                                  InterruptedTaskException,
                                                                  FileChannelException {
        List<K> keyList = internalTreeNode.getKeyList(bTreeDegree);

        // If it's the root and has no keys, return.
        if (index == path.size() - 1 && keyList.isEmpty()) {
            return;
        }

        // Find the index of the key to delete.
        int indexOfKey = keyList.indexOf(identifier);
        if (indexOfKey != -1) {
            if (internalTreeNode.isRoot()) {
                // Handle deletion in root.
                this.fillRootAtIndex(internalTreeNode, indexOfKey, identifier, bTreeDegree);
            } else {
                // Delete the key from the internal node.
                this.deleteInternalNode((InternalTreeNode<K>) path.get(index + 1), internalTreeNode, indexOfKey, bTreeDegree);
            }
        }

        int nodeKeySize = internalTreeNode.getKeyList(bTreeDegree).size();
        // Check if under filled and balance the tree if necessary.
        if (nodeKeySize < minKeys && !internalTreeNode.isRoot()) {
            InternalTreeNode<K> parent = (InternalTreeNode<K>) path.get(index + 1);
            this.fillNode(internalTreeNode, parent, parent.getIndexOfChild(internalTreeNode.getPointer()), bTreeDegree);
        }
    }

    /**
     * Handles deletion in the root node by replacing the key with an appropriate key from its subtree.
     *
     * @param internalTreeNode The root node.
     * @param indexOfKey       The index of the key to delete.
     * @param identifier       The key being deleted.
     * @param bTreeDegree      The degree of the tree.
     * @throws StorageException If an error occurs during storage operations.
     * @throws BTreeException   If an error occurs during deletion.
     */
    private void fillRootAtIndex(InternalTreeNode<K> internalTreeNode, int indexOfKey, K identifier,
                                 int bTreeDegree) throws
                                                  StorageException,
                                                  BTreeException,
                                                  SerializationException,
                                                  InterruptedTaskException,
                                                  FileChannelException {
        // Get the child node where the replacement key can be found.
        LeafTreeNode<K, ?> leafTreeNode = BTreeUtils.getResponsibleNode(session.getIndexStorageManager(), session.read(internalTreeNode.getChildAtIndex(indexOfKey + 1)), identifier, indexId, bTreeDegree, nodeFactory, vBinaryObjectFactory);

        // Replace the key in the root node.
        internalTreeNode.setKey(indexOfKey, leafTreeNode.getKeyList(bTreeDegree).getLast());
        session.update(internalTreeNode);
    }

    /**
     * Balances an under filled node by borrowing keys from siblings or merging nodes.
     * This method is called when a node has fewer keys than the minimum required after a deletion.
     * It attempts to balance the tree by borrowing from siblings or merging nodes if necessary.
     *
     * @param currentNode The under filled node.
     * @param parentNode  The parent node.
     * @param index       The index of the current node in the parent's child list.
     * @param bTreeDegree The degree of the tree.
     * @throws BTreeException   If an error occurs during balancing.
     * @throws StorageException If an error occurs during storage operations.
     */
    private void fillNode(AbstractTreeNode<K> currentNode, InternalTreeNode<K> parentNode, int index,
                          int bTreeDegree) throws
                                           BTreeException,
                                           StorageException,
                                           SerializationException,
                                           InterruptedTaskException,
                                           FileChannelException {
        boolean borrowed;

        // The first node is a leaf node.
        if (index == 0) {
            // If it is the first child, we can only try to borrow from the right sibling.
            borrowed = tryBorrowRight(parentNode, index, currentNode, bTreeDegree);
        } else {
            // If it's not the first child, first try to borrow from the left sibling.
            borrowed = tryBorrowLeft(parentNode, index, currentNode, bTreeDegree);

            // If borrowing from the left sibling wasn't possible and the node is not the last child, we can try to borrow from the right sibling.
            if (!borrowed && index < parentNode.getKeyList(bTreeDegree).size() - 1) {
                borrowed = tryBorrowRight(parentNode, index, currentNode, bTreeDegree);
            }
        }

        // If borrowing isn't possible, merge the node with a sibling.
        if (!borrowed) {
            merge(parentNode, currentNode, index, bTreeDegree);
        }
    }

    /**
     * Attempts to borrow a key from the right sibling of a node.
     *
     * @param parentNode  The parent node.
     * @param idx         The index of the current node in the parent's child list.
     * @param child       The under filled node.
     * @param bTreeDegree The degree of the tree.
     * @return True if borrowing was successful; false otherwise.
     * @throws StorageException If an error occurs during storage operations.
     * @throws BTreeException   If an error occurs during balancing.
     */
    private boolean tryBorrowRight(InternalTreeNode<K> parentNode, int idx, AbstractTreeNode<K> child,
                                   int bTreeDegree) throws
                                                    StorageException,
                                                    BTreeException,
                                                    SerializationException,
                                                    InterruptedTaskException,
                                                    FileChannelException {
        // Get the right sibling.
        AbstractTreeNode<K> sibling = session.read(parentNode.getChildrenList().get(idx + 1));

        // Check if the sibling has enough keys to lend.
        if (sibling.getKeyList(bTreeDegree, vBinaryObjectFactory.size()).size() > minKeys) {
            this.borrowFromNext(parentNode, idx, child, bTreeDegree);
            return true;
        }
        return false;
    }

    /**
     * Attempts to borrow a key from the left sibling of a node.
     *
     * @param parentNode  The parent node.
     * @param idx         The index of the current node in the parent's child list.
     * @param child       The under filled node.
     * @param bTreeDegree The degree of the tree.
     * @return True if borrowing was successful; false otherwise.
     * @throws StorageException If an error occurs during storage operations.
     * @throws BTreeException   If an error occurs during balancing.
     */
    private boolean tryBorrowLeft(InternalTreeNode<K> parentNode, int idx, AbstractTreeNode<K> child,
                                  int bTreeDegree) throws
                                                   StorageException,
                                                   BTreeException,
                                                   SerializationException,
                                                   InterruptedTaskException,
                                                   FileChannelException {
        // Get the left sibling.
        AbstractTreeNode<K> sibling = session.read(parentNode.getChildrenList().get(idx - 1));

        // Check if the sibling has enough keys to lend.
        if (sibling.getKeyList(bTreeDegree, vBinaryObjectFactory.size()).size() > minKeys) {
            this.borrowFromPrev(parentNode, idx, child, bTreeDegree);
            return true;
        }
        return false;
    }

    /**
     * Borrows a key from the left sibling and adjusts pointers accordingly.
     * This is to maintain balance, because it ensures that the node has enough keys to meet the minimum number of keys required.
     *
     * @param parent        The parent node.
     * @param index         The index of the current node in the parent's child list.
     * @param optionalChild The under filled node.
     * @throws StorageException If an error occurs during storage operations.
     * @throws BTreeException   If an error occurs during balancing.
     */
    private void borrowFromPrev(InternalTreeNode<K> parent, int index, AbstractTreeNode<K> optionalChild,
                                int bTreeDegree) throws
                                                 StorageException,
                                                 BTreeException,
                                                 SerializationException,
                                                 InterruptedTaskException,
                                                 FileChannelException {
        AbstractTreeNode<K> child = optionalChild != null ? optionalChild : session.read(parent.getChildrenList().get(index));
        AbstractTreeNode<K> sibling = session.read(parent.getChildrenList().get(index - 1));

        // If the child is an internal node, we remove the last pointer of its sibling, this is to maintain order because that pointer has the largest keys in the sibling node.
        if (!child.isLeaf()) {
            InternalTreeNode<K> siblingInternalNode = (InternalTreeNode<K>) sibling;
            List<InternalTreeNode.ChildPointers<K>> childPointersList = new ArrayList<>(siblingInternalNode.getChildPointersList(bTreeDegree));
            InternalTreeNode.ChildPointers<K> siblingLastChildPointer = childPointersList.removeLast();

            siblingInternalNode.setChildPointers(childPointersList, bTreeDegree, true);

            // This corresponds to the key that is currently the separator between child nodes.
            // Since we're moving a key from the sibling to the child, we need to adjust the parent key to reflect the new separation.
            K currKey = parent.getKeyList(bTreeDegree).get(index - 1);
            parent.setKey(index - 1, siblingLastChildPointer.getKey());


            // If the child still has keys, then we adjust the sibling's borrowed child pointer to fit into the child node.
            // The removed sibling's child pointer is set to be on the left side of the newly added key.
            // If the child has no keys, it's possible that previous operations (like merges or deletions) have left the node without keys.
            InternalTreeNode<K> childInternalNode = (InternalTreeNode<K>) child;
            if (!childInternalNode.getKeyList(bTreeDegree).isEmpty()) {
                ArrayList<InternalTreeNode.ChildPointers<K>> childPointersList2 = new ArrayList<>(childInternalNode.getChildPointersList(bTreeDegree));
                siblingLastChildPointer.setRight(childPointersList2.getFirst().getLeft());
                siblingLastChildPointer.setKey(currKey);
                siblingLastChildPointer.setLeft(siblingLastChildPointer.getRight());
                childPointersList2.add(siblingLastChildPointer);
                childInternalNode.setChildPointers(childPointersList2, bTreeDegree, false);
            } else {
                Pointer first = childInternalNode.getChildrenList().getFirst();
                childInternalNode.addChildPointers(currKey, siblingLastChildPointer.getRight(), first, bTreeDegree, true);
            }
        } else {
            // Borrowing from leaf nodes is done by simply removing the value from the sibling and adding it to the child, there are no pointers to adjust.
            LeafTreeNode<K, V> siblingLeafNode = (LeafTreeNode<K, V>) sibling;
            LeafTreeNode<K, V> childLeafNode = (LeafTreeNode<K, V>) child;

            List<LeafTreeNode.KeyValue<K, V>> keyValueList = new ArrayList<>(siblingLeafNode.getKeyValueList(bTreeDegree));
            LeafTreeNode.KeyValue<K, V> keyValue = keyValueList.removeLast();
            siblingLeafNode.setKeyValues(keyValueList, bTreeDegree);

            parent.setKey(index - 1, keyValue.key());
            childLeafNode.addKeyValue(keyValue, bTreeDegree);
        }
        session.update(parent);
        session.update(child);
        session.update(sibling);
    }

    /**
     * Borrows a key from the right sibling and adjusts pointers accordingly.
     * This is to maintain balance, because it ensures that the node has enough keys to meet the minimum number of keys required.
     *
     * @param parent        The parent node.
     * @param index         The index of the current node in the parent's child list.
     * @param optionalChild The under filled node.
     * @throws StorageException If an error occurs during storage operations.
     * @throws BTreeException   If an error occurs during balancing.
     */
    private void borrowFromNext(InternalTreeNode<K> parent, int index, AbstractTreeNode<K> optionalChild,
                                int bTreeDegree) throws
                                                 StorageException,
                                                 BTreeException,
                                                 SerializationException,
                                                 InterruptedTaskException,
                                                 FileChannelException {
        AbstractTreeNode<K> child = optionalChild != null ? optionalChild : session.read(parent.getChildrenList().get(index));
        AbstractTreeNode<K> sibling = session.read(parent.getChildrenList().get(index + 1));

        // If the child is an internal node, we remove the first pointer of its sibling, this is to maintain order because that pointer has the smallest keys in the sibling node.
        if (!child.isLeaf()) {
            InternalTreeNode<K> siblingInternalNode = (InternalTreeNode<K>) sibling;
            List<InternalTreeNode.ChildPointers<K>> siblingPointersList = new ArrayList<>(siblingInternalNode.getChildPointersList(bTreeDegree));
            InternalTreeNode.ChildPointers<K> siblingFirstChildPointer = siblingPointersList.removeFirst();

            siblingInternalNode.setChildPointers(siblingPointersList, bTreeDegree, true);

            // This corresponds to the key that is currently the separator between child nodes.
            // Since we're moving a key from the sibling to the child, we need to adjust the parent key to reflect the new separation.
            K currKey = parent.getKeyList(bTreeDegree).get(index);
            parent.setKey(index, siblingFirstChildPointer.getKey());

            InternalTreeNode<K> childInternalNode = (InternalTreeNode<K>) child;

            // If the child still has keys, then we adjust the sibling's borrowed child pointer to fit into the child node.
            // The removed sibling's child pointer is set to be on the right side of the newly added key.
            // If the child has no keys, it's possible that previous operations (like merges or deletions) have left the node without keys.
            if (!childInternalNode.getKeyList(bTreeDegree).isEmpty()) {
                ArrayList<InternalTreeNode.ChildPointers<K>> childPointersList2 = new ArrayList<>(childInternalNode.getChildPointersList(bTreeDegree));
                siblingFirstChildPointer.setRight(siblingFirstChildPointer.getLeft());
                siblingFirstChildPointer.setKey(currKey);
                siblingFirstChildPointer.setLeft(childPointersList2.getLast().getRight());
                childPointersList2.add(siblingFirstChildPointer);
                childInternalNode.setChildPointers(childPointersList2, bTreeDegree, false);
            } else {
                Pointer first = childInternalNode.getChildrenList().getFirst();
                childInternalNode.addChildPointers(currKey, first, siblingFirstChildPointer.getLeft(), bTreeDegree, true);
            }
        } else {
            // Borrowing from leaf nodes is done by simply removing the value from the sibling and adding it to the child, there are no pointers to adjust.
            LeafTreeNode<K, V> siblingLeafNode = (LeafTreeNode<K, V>) sibling;
            LeafTreeNode<K, V> childLeafNode = (LeafTreeNode<K, V>) child;

            List<LeafTreeNode.KeyValue<K, V>> keyValueList = new ArrayList<>(siblingLeafNode.getKeyValueList(bTreeDegree));
            LeafTreeNode.KeyValue<K, V> keyValue = keyValueList.removeFirst();
            siblingLeafNode.setKeyValues(keyValueList, bTreeDegree);

            parent.setKey(index, keyValueList.getFirst().key());
            childLeafNode.addKeyValue(keyValue, bTreeDegree);
        }

        session.update(parent);
        session.update(child);
        session.update(sibling);
    }

    /**
     * Merges the under filled node with a sibling node.
     *
     * @param parent      The parent node.
     * @param child       The under filled node.
     * @param index       The index of the current node in the parent's child list.
     * @param bTreeDegree The degree of the tree.
     * @throws StorageException If an error occurs during storage operations.
     * @throws BTreeException   If an error occurs during merging.
     */
    private void merge(InternalTreeNode<K> parent, AbstractTreeNode<K> child, int index, int bTreeDegree) throws
                                                                                                          StorageException,
                                                                                                          BTreeException,
                                                                                                          SerializationException,
                                                                                                          InterruptedTaskException,
                                                                                                          FileChannelException {
        // We need to decide which sibling is to be merged with under filled node.
        // By default, we first try the right sibling, if it's not possible we try with the left sibling.
        int siblingIndex = index + 1;
        if (index == parent.getChildrenList().size() - 1) {
            siblingIndex = index - 1;
        }

        AbstractTreeNode<K> sibling = session.read(parent.getChildrenList().get(siblingIndex));
        // Hold a reference to the node that needs to be deleted after merging.
        AbstractTreeNode<K> toRemove = sibling;

        if (sibling.getKeyList(bTreeDegree, vBinaryObjectFactory.size()).size() > child.getKeyList(bTreeDegree, vBinaryObjectFactory.size()).size()) {
            // The merge always happens into the sibling that has more keys. This means that the node that will be deleted after merging is the child.
            AbstractTreeNode<K> temp = child;
            child = sibling;
            sibling = temp;
            toRemove = sibling;
            int tempSib = siblingIndex;
            siblingIndex = index;
            index = tempSib;
        }

        // When dealing with internal nodes, we add the sibling keys into the child.
        // Then we merge the child pointers, the sibling child pointer can be either prepended or appended, depending on if the
        // sibling is before or after the child.
        // If we're dealing with a leaf node, the same process applies, but only for the keys.
        ArrayList<LeafTreeNode.KeyValue<K, V>> keyValueListToMove = new ArrayList<>();
        ArrayList<Pointer> childPointersToMove = new ArrayList<>();

        if (!child.isLeaf()) {
            InternalTreeNode<K> childInternalTreeNode = (InternalTreeNode<K>) child;
            List<K> childKeyList = new ArrayList<>(childInternalTreeNode.getKeyList(bTreeDegree));
            childKeyList.addAll(sibling.getKeyList(bTreeDegree, vBinaryObjectFactory.size()));
            childKeyList.sort(K::compareTo);
            childInternalTreeNode.setKeys(childKeyList);

            childPointersToMove = new ArrayList<>(childInternalTreeNode.getChildrenList());
            if (index > siblingIndex) {
                childPointersToMove.addAll(0, ((InternalTreeNode<K>) sibling).getChildrenList());
            } else {
                childPointersToMove.addAll(((InternalTreeNode<K>) sibling).getChildrenList());
            }
        } else {
            LeafTreeNode<K, V> childLeafTreeNode = (LeafTreeNode<K, V>) child;
            keyValueListToMove = new ArrayList<>(childLeafTreeNode.getKeyValueList(bTreeDegree));
            keyValueListToMove.addAll(((LeafTreeNode<K, V>) sibling).getKeyValueList(bTreeDegree));
            Collections.sort(keyValueListToMove);
        }

        int keyToRemoveIndex = siblingIndex == 0 ? siblingIndex : siblingIndex - 1;
        K parentKeyAtIndex = parent.getKeyList(bTreeDegree).get(keyToRemoveIndex);

        // After merging child and sibling, the parent node needs to remove the key that separated them and remove the pointer to the now redundant node (sibling).
        // This is because the merged node now contains all keys that were previously split between child and sibling.
        parent.removeKey(keyToRemoveIndex, bTreeDegree);
        parent.removeChild(siblingIndex, bTreeDegree);

        // After removing the key, we need to check if the parent node is now under filled or empty after removal.
        if (parent.getKeyList(bTreeDegree).isEmpty()) {
            if (!child.isLeaf()) {
                if (!(child instanceof InternalTreeNode)) {
                    throw new BTreeException(DbError.NODE_MISMATCH_ERROR, "Node marked as leaf is not a leaf node");
                }
                // Move the separating key down to the child.
                ((InternalTreeNode<K>) child).addKey(parentKeyAtIndex, bTreeDegree);
            }
            // If the parent node was the root, we need to pivot to the child, because the parent now has no keys left.
            if (parent.isRoot()) {
                child.setAsRoot();
                parent.unsetAsRoot();
            }
            session.remove(parent);
        } else {
            session.update(parent);
        }

        if (child.isLeaf()) {
            ((LeafTreeNode<K, V>) child).setKeyValues(keyValueListToMove, bTreeDegree);
        } else {
            ((InternalTreeNode<K>) child).setChildren(childPointersToMove);
        }
        session.update(child);

        // If the node being removed is a leaf node, we need to maintain consistency by fixing the sibling pointers, so that the siblings don't point
        // to a node that no longer exists.
        if (toRemove.isLeaf()) {
            if (!(toRemove instanceof LeafTreeNode<?, ?>)) {
                throw new BTreeException(DbError.NODE_MISMATCH_ERROR, "Node marked as leaf is not a leaf node");
            }
            this.fixSiblingPointers((LeafTreeNode<K, V>) toRemove, bTreeDegree);
        }
        session.remove(toRemove);
    }

    /**
     * Updates sibling pointers of leaf nodes after a node has been removed.
     * This ensures node links remain intact. Otherwise, the traversal of the tree would not work correctly.
     *
     * @param node The leaf node that was removed.
     * @throws StorageException If an error occurs during storage operations.
     */
    private void fixSiblingPointers(LeafTreeNode<K, V> node, int bTreeDegree) throws StorageException,
                                                                                     InterruptedTaskException,
                                                                                     FileChannelException {
        // Get the next and previous sibling pointers.
        Optional<Pointer> optionalNextSiblingPointer = node.getNextSiblingPointer(bTreeDegree);
        Optional<Pointer> optionalPreviousSiblingPointer = node.getPreviousSiblingPointer(bTreeDegree);
        if (optionalNextSiblingPointer.isPresent()) {
            // Update the previous pointer of the next sibling.
            LeafTreeNode<K, V> nextNode = (LeafTreeNode<K, V>) session.read(optionalNextSiblingPointer.get());
            nextNode.setPreviousSiblingPointer(optionalPreviousSiblingPointer.orElseGet(Pointer::empty), bTreeDegree);
            session.update(nextNode);
        }

        if (optionalPreviousSiblingPointer.isPresent()) {
            // Update the next pointer of the previous sibling.
            LeafTreeNode<K, ?> previousNode = (LeafTreeNode<K, ?>) session.read(optionalPreviousSiblingPointer.get());
            previousNode.setNextSiblingPointer(optionalNextSiblingPointer.orElseGet(Pointer::empty), bTreeDegree);
            session.update(previousNode);
        }
    }
}