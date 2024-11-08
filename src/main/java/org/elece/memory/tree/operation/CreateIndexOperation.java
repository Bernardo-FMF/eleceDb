package org.elece.memory.tree.operation;

import org.elece.config.DbConfig;
import org.elece.exception.*;
import org.elece.memory.KeyValueSize;
import org.elece.memory.Pointer;
import org.elece.memory.data.BinaryObjectFactory;
import org.elece.memory.tree.node.AbstractTreeNode;
import org.elece.memory.tree.node.InternalTreeNode;
import org.elece.memory.tree.node.LeafTreeNode;
import org.elece.storage.index.session.Session;
import org.elece.utils.BTreeUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Handles the insertion of a key-value pair (index) from a tree.
 *
 * @param <K> The type of keys.
 * @param <V> The type of values associated with the keys.
 */
public class CreateIndexOperation<K extends Comparable<K>, V> {
    private final DbConfig dbConfig;
    private final Session<K> session;
    private final BinaryObjectFactory<K> binaryObjectKeyFactory;
    private final BinaryObjectFactory<V> binaryObjectValueFactory;
    private final KeyValueSize keyValueSize;

    public CreateIndexOperation(DbConfig dbConfig, Session<K> session,
                                BinaryObjectFactory<K> binaryObjectKeyFactory,
                                BinaryObjectFactory<V> binaryObjectValueFactory, KeyValueSize keyValueSize) {
        this.dbConfig = dbConfig;
        this.session = session;
        this.binaryObjectKeyFactory = binaryObjectKeyFactory;
        this.binaryObjectValueFactory = binaryObjectValueFactory;
        this.keyValueSize = keyValueSize;
    }

    public AbstractTreeNode<K> addIndex(AbstractTreeNode<K> root, K identifier, V value) throws BTreeException,
                                                                                                StorageException,
                                                                                                SerializationException,
                                                                                                InterruptedTaskException,
                                                                                                FileChannelException {
        List<AbstractTreeNode<K>> path = new LinkedList<>();
        int bTreeDegree = dbConfig.getBTreeDegree();

        // Find the path to the leaf node responsible for the key.
        BTreeUtils.getPathToResponsibleNode(session, path, root, identifier, bTreeDegree);

        // Variables to keep track of the key and child node to be passed up to parent nodes if splits occur.
        K idForParentToStore = identifier;
        AbstractTreeNode<K> newChildForParent = null;
        // The node where the key-value pair will be stored.
        AbstractTreeNode<K> newNode = null;

        for (int index = 0; index < path.size(); index++) {
            AbstractTreeNode<K> currentNode = path.get(index);

            // If we're at the leaf node where the key should be inserted.
            if (index == 0) {
                // Current node is a leaf which should handle storing the data.
                List<K> currentNodeKeyList = currentNode.getKeyList(bTreeDegree, binaryObjectValueFactory.size());

                // Check if the key already exists to prevent duplicates.
                if (currentNodeKeyList.contains(identifier)) {
                    throw new BTreeException(DbError.DUPLICATE_INDEX_INSERTION_ERROR, String.format("Indexed key '%s' already exists", identifier));
                }

                // If there's space in the current node, insert the key-value pair.
                if (currentNodeKeyList.size() < (bTreeDegree - 1)) {
                    ((LeafTreeNode<K, V>) currentNode).addKeyValue(identifier, value, bTreeDegree);
                    session.write(currentNode);
                    session.commit();
                    return currentNode;
                }

                // If the node is full, split the node.
                // Create a new sibling leaf node to accommodate the split.
                LeafTreeNode<K, V> newSiblingLeafNode = new LeafTreeNode<>(session.getIndexStorageManager().getEmptyNode(this.keyValueSize), binaryObjectKeyFactory, binaryObjectValueFactory);

                // Add the key-value pair and split the node, obtaining the key-values to pass to the new node.
                List<LeafTreeNode.KeyValue<K, V>> passingKeyValues = ((LeafTreeNode<K, V>) currentNode).addAndSplit(identifier, value, bTreeDegree);

                // Set the key-values in the new sibling node, and persist this change.
                newSiblingLeafNode.setKeyValues(passingKeyValues, bTreeDegree);
                session.write(newSiblingLeafNode);

                // Fix the sibling pointers between the current node and the new sibling.
                fixSiblingPointers((LeafTreeNode<K, V>) currentNode, newSiblingLeafNode, bTreeDegree);
                session.write(newSiblingLeafNode);
                session.write(currentNode);

                // Determine which node now contains the key-value pair.
                newNode = currentNodeKeyList.contains(identifier) ? currentNode : newSiblingLeafNode;

                // If the current node was the root (no parent), create a new root node.
                if (path.size() == 1) {
                    currentNode.unsetAsRoot();
                    InternalTreeNode<K> newRoot = new InternalTreeNode<>(session.getIndexStorageManager().getEmptyNode(this.keyValueSize), binaryObjectKeyFactory);
                    newRoot.setAsRoot();

                    // Add child pointers to the new root node.
                    newRoot.addChildPointers(passingKeyValues.getFirst().key(), currentNode.getPointer(), newSiblingLeafNode.getPointer(), bTreeDegree, false);
                    session.write(newRoot);
                    session.write(currentNode);
                    session.commit();
                    return newNode;
                }

                // Prepare to pass the new child and key up to the parent node.
                newChildForParent = newSiblingLeafNode;
                idForParentToStore = passingKeyValues.getFirst().key();
            } else {
                // We're at an internal node in the path.
                InternalTreeNode<K> currentInternalTreeNode = (InternalTreeNode<K>) currentNode;

                // If the internal node has space for a new key.
                if (currentInternalTreeNode.getKeyList(bTreeDegree).size() < bTreeDegree - 1) {
                    // Add the key to the internal node.
                    int indexOfAddedKey = currentInternalTreeNode.addKey(idForParentToStore, bTreeDegree);

                    // Determine where to insert the new child pointer.
                    if (newChildForParent.getKeyList(bTreeDegree, binaryObjectValueFactory.size()).getFirst().compareTo(idForParentToStore) < 0) {
                        // Insert the child pointer at the index of the added key.
                        currentInternalTreeNode.addChildAtIndex(indexOfAddedKey, newChildForParent.getPointer());
                    } else {
                        // Insert the child pointer after the index of the added key.
                        currentInternalTreeNode.addChildAtIndex(indexOfAddedKey + 1, newChildForParent.getPointer());
                    }
                    session.write(currentInternalTreeNode);
                    session.commit();
                    return newNode;
                }

                // If the internal node is full, split the node.
                // Split the internal node and obtain the child pointers to pass up.
                List<InternalTreeNode.ChildPointers<K>> passingChildPointers = currentInternalTreeNode.addAndSplit(idForParentToStore, newChildForParent.getPointer(), bTreeDegree);

                // Get the first child pointer to pass up to the parent.
                InternalTreeNode.ChildPointers<K> firstPassingChildPointers = passingChildPointers.getFirst();
                idForParentToStore = firstPassingChildPointers.getKey();
                // Remove the key being passed up from the list.
                passingChildPointers.removeFirst();

                InternalTreeNode<K> newInternalSibling = new InternalTreeNode<>(session.getIndexStorageManager().getEmptyNode(this.keyValueSize), binaryObjectKeyFactory);

                // Set the child pointers in the new sibling node.
                newInternalSibling.setChildPointers(passingChildPointers, bTreeDegree, true);
                session.write(newInternalSibling);

                // If the current internal node was the root, create a new root node.
                if (currentInternalTreeNode.isRoot()) {
                    currentInternalTreeNode.unsetAsRoot();
                    InternalTreeNode<K> newRoot = new InternalTreeNode<>(session.getIndexStorageManager().getEmptyNode(this.keyValueSize), binaryObjectKeyFactory);
                    newRoot.setAsRoot();

                    // Add child pointers to the new root node.
                    newRoot.addChildPointers(idForParentToStore, currentNode.getPointer(), newInternalSibling.getPointer(), bTreeDegree, false);
                    session.write(newRoot);
                    session.write(currentInternalTreeNode);
                    session.commit();
                    return newNode;
                } else {
                    session.write(currentInternalTreeNode);
                }
            }
        }

        throw new BTreeException(DbError.FAILED_TO_INSERT_INDEX_ERROR, String.format("Failed to insert key '%s' into the tree", identifier));
    }

    private void fixSiblingPointers(LeafTreeNode<K, V> currentNode, LeafTreeNode<K, V> newLeafTreeNode,
                                    int bTreeDegree) throws StorageException, InterruptedTaskException,
                                                            FileChannelException {
        // Get the pointer to the next sibling of the current node.
        Optional<Pointer> currentNodeNextSiblingPointer = currentNode.getNextSiblingPointer(bTreeDegree);

        // Update the current node's next sibling pointer to point to the new sibling.
        currentNode.setNextSiblingPointer(newLeafTreeNode.getPointer(), bTreeDegree);

        // Set the new sibling's previous sibling pointer to the current node.
        newLeafTreeNode.setPreviousSiblingPointer(currentNode.getPointer(), bTreeDegree);

        // If the current node had a next sibling, update pointers accordingly.
        if (currentNodeNextSiblingPointer.isPresent()) {
            // Set the new sibling's next sibling pointer to the current node's next sibling.
            newLeafTreeNode.setNextSiblingPointer(currentNodeNextSiblingPointer.get(), bTreeDegree);

            LeafTreeNode<K, V> currentNextSibling = (LeafTreeNode<K, V>) session.read(currentNodeNextSiblingPointer.get());

            // Update the next sibling's previous sibling pointer to point to the new sibling.
            currentNextSibling.setPreviousSiblingPointer(newLeafTreeNode.getPointer(), bTreeDegree);
            session.write(currentNextSibling);
        }
    }
}
