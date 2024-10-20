package org.elece.index;

import org.elece.config.DbConfig;
import org.elece.exception.RuntimeDbException;
import org.elece.exception.btree.BTreeException;
import org.elece.exception.btree.type.IndexNotFoundError;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.storage.StorageException;
import org.elece.memory.KeyValueSize;
import org.elece.memory.Pointer;
import org.elece.memory.data.BinaryObjectFactory;
import org.elece.memory.tree.node.*;
import org.elece.memory.tree.operation.CreateIndexOperation;
import org.elece.memory.tree.operation.DeleteIndexOperation;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.NodeData;
import org.elece.storage.index.session.AtomicIOSession;
import org.elece.storage.index.session.factory.IOSessionFactory;
import org.elece.utils.BTreeUtils;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class TreeIndexManager<K extends Comparable<K>, V> extends AbstractTreeIndexManager<K, V> {
    private final IndexStorageManager indexStorageManager;
    private final IOSessionFactory IOSessionFactory;
    private final DbConfig dbConfig;
    private final BinaryObjectFactory<K> kBinaryObjectFactory;
    private final BinaryObjectFactory<V> vBinaryObjectFactory;
    private final NodeFactory<K> nodeFactory;
    protected final KeyValueSize keyValueSize;

    public TreeIndexManager(int indexId, IndexStorageManager indexStorageManager, IOSessionFactory iOSessionFactory, DbConfig dbConfig, BinaryObjectFactory<K> kBinaryObjectFactory, BinaryObjectFactory<V> vBinaryObjectFactory, NodeFactory<K> nodeFactory) {
        super(indexId);
        this.indexStorageManager = indexStorageManager;
        this.IOSessionFactory = iOSessionFactory;
        this.dbConfig = dbConfig;
        this.kBinaryObjectFactory = kBinaryObjectFactory;
        this.vBinaryObjectFactory = vBinaryObjectFactory;
        this.nodeFactory = nodeFactory;
        this.keyValueSize = new KeyValueSize(kBinaryObjectFactory.size(), vBinaryObjectFactory.size());
    }

    @Override
    public void addIndex(K identifier, V value) throws BTreeException, StorageException, SerializationException {
        AtomicIOSession<K> atomicIOSession = this.IOSessionFactory.create(indexStorageManager, indexId, nodeFactory, keyValueSize);
        AbstractTreeNode<K> root = getRoot(atomicIOSession);
        new CreateIndexOperation<>(dbConfig, atomicIOSession, kBinaryObjectFactory, vBinaryObjectFactory, keyValueSize).addIndex(root, identifier, value);
    }

    @Override
    public void updateIndex(K identifier, V value) throws BTreeException, StorageException, SerializationException {
        AtomicIOSession<K> atomicIOSession = this.IOSessionFactory.create(indexStorageManager, indexId, nodeFactory, keyValueSize);

        int bTreeDegree = dbConfig.getBTreeDegree();
        LeafTreeNode<K, V> node = BTreeUtils.getResponsibleNode(indexStorageManager, getRoot(atomicIOSession), identifier, indexId, bTreeDegree, nodeFactory, vBinaryObjectFactory);
        List<K> keyList = node.getKeyList(bTreeDegree);
        if (!keyList.contains(identifier)) {
            throw new BTreeException(new IndexNotFoundError());
        }

        node.setKeyValue(keyList.indexOf(identifier), new LeafTreeNode.KeyValue<>(identifier, value));
        atomicIOSession.update(node);
        atomicIOSession.commit();
    }

    @Override
    public Optional<V> getIndex(K identifier) throws BTreeException, StorageException {
        AtomicIOSession<K> atomicIOSession = this.IOSessionFactory.create(indexStorageManager, indexId, nodeFactory, keyValueSize);

        int bTreeDegree = dbConfig.getBTreeDegree();
        LeafTreeNode<K, V> baseTreeNode = BTreeUtils.getResponsibleNode(indexStorageManager, getRoot(atomicIOSession), identifier, indexId, bTreeDegree, nodeFactory, vBinaryObjectFactory);
        for (LeafTreeNode.KeyValue<K, V> entry : baseTreeNode.getKeyValueList(bTreeDegree)) {
            if (entry.key().compareTo(identifier) == 0) {
                return Optional.of(entry.value());
            }
        }

        return Optional.empty();
    }

    @Override
    public boolean removeIndex(K identifier) throws BTreeException, StorageException, SerializationException {
        AtomicIOSession<K> atomicIOSession = this.IOSessionFactory.create(indexStorageManager, indexId, nodeFactory, keyValueSize);
        AbstractTreeNode<K> root = getRoot(atomicIOSession);
        return new DeleteIndexOperation<>(dbConfig, atomicIOSession, vBinaryObjectFactory, nodeFactory, indexId).removeIndex(root, identifier);
    }

    @Override
    public void purgeIndex() throws IOException, ExecutionException, InterruptedException {
        if (this.indexStorageManager.supportsPurge()) {
            this.indexStorageManager.purgeIndex(indexId);
        }
    }

    @Override
    public LockableIterator<LeafTreeNode.KeyValue<K, V>> getSortedIterator() throws StorageException {
        AtomicIOSession<K> atomicIOSession = this.IOSessionFactory.create(indexStorageManager, indexId, nodeFactory, keyValueSize);

        return new LockableIterator<>() {
            private int keyIndex = 0;
            LeafTreeNode<K, V> currentLeaf = getFarLeftLeaf();

            @Override
            public void lock() {
            }

            @Override
            public void unlock() {
            }

            @Override
            public boolean hasNext() {
                int size = currentLeaf.getKeyList(dbConfig.getBTreeDegree()).size();
                if (keyIndex == size) {
                    return currentLeaf.getNextSiblingPointer(dbConfig.getBTreeDegree()).isPresent();
                }
                return true;
            }

            @Override
            public LeafTreeNode.KeyValue<K, V> next() {
                List<LeafTreeNode.KeyValue<K, V>> keyValueList = currentLeaf.getKeyValueList(dbConfig.getBTreeDegree());

                if (keyIndex == keyValueList.size()) {
                    try {
                        currentLeaf = (LeafTreeNode<K, V>) atomicIOSession.read(currentLeaf.getNextSiblingPointer(dbConfig.getBTreeDegree()).get());
                    } catch (StorageException exception) {
                        throw new RuntimeDbException(exception.getDbError());
                    }
                    keyIndex = 0;
                    keyValueList = currentLeaf.getKeyValueList(dbConfig.getBTreeDegree());
                }

                LeafTreeNode.KeyValue<K, V> output = keyValueList.get(keyIndex);
                keyIndex += 1;
                return output;
            }
        };
    }

    @Override
    public K getLastIndex() throws StorageException {
        LeafTreeNode<K, V> farRightLeaf = getFarRightLeaf();
        return farRightLeaf.getKeyList(dbConfig.getBTreeDegree()).getLast();
    }

    private AbstractTreeNode<K> getRoot(AtomicIOSession<K> atomicIOSession) throws StorageException {
        Optional<AbstractTreeNode<K>> optionalRoot = atomicIOSession.getRoot();
        if (optionalRoot.isPresent()) {
            return optionalRoot.get();
        }

        byte[] emptyNode = indexStorageManager.getEmptyNode(keyValueSize);
        LeafTreeNode<K, ?> leafTreeNode = (LeafTreeNode<K, ?>) nodeFactory.fromBytes(emptyNode, NodeType.LEAF);
        leafTreeNode.setAsRoot();

        NodeData nodeData = atomicIOSession.write(leafTreeNode);
        leafTreeNode.setPointer(nodeData.pointer());
        return leafTreeNode;
    }

    protected LeafTreeNode<K, V> getFarLeftLeaf() throws StorageException {
        AtomicIOSession<K> indexIOSession = this.IOSessionFactory.create(indexStorageManager, indexId, nodeFactory, keyValueSize);
        AbstractTreeNode<K> root = getRoot(indexIOSession);
        if (root.isLeaf()) {
            return (LeafTreeNode<K, V>) root;
        }

        AbstractTreeNode<K> farLeftChild = root;

        while (!farLeftChild.isLeaf()) {
            farLeftChild = indexIOSession.read(((InternalTreeNode<K>) farLeftChild).getChildAtIndex(0));
        }

        return (LeafTreeNode<K, V>) farLeftChild;
    }

    protected LeafTreeNode<K, V> getFarRightLeaf() throws StorageException {
        AtomicIOSession<K> indexIOSession = this.IOSessionFactory.create(indexStorageManager, indexId, nodeFactory, keyValueSize);
        AbstractTreeNode<K> root = getRoot(indexIOSession);
        if (root.isLeaf()) {
            return (LeafTreeNode<K, V>) root;
        }

        AbstractTreeNode<K> farRightChild = root;

        while (!farRightChild.isLeaf()) {
            List<Pointer> childrenList = ((InternalTreeNode<K>) farRightChild).getChildrenList();
            farRightChild = indexIOSession.read(childrenList.getLast());
        }

        return (LeafTreeNode<K, V>) farRightChild;
    }
}
