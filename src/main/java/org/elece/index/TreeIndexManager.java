package org.elece.index;

import org.elece.config.DbConfig;
import org.elece.exception.btree.BTreeException;
import org.elece.exception.btree.type.IndexNotFoundError;
import org.elece.exception.storage.StorageException;
import org.elece.memory.KeyValueSize;
import org.elece.memory.tree.node.AbstractTreeNode;
import org.elece.memory.tree.node.LeafTreeNode;
import org.elece.memory.tree.node.NodeFactory;
import org.elece.memory.tree.node.NodeType;
import org.elece.memory.tree.node.data.BinaryObjectFactory;
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
    public AbstractTreeNode<K> addIndex(K identifier, V value) throws BTreeException, StorageException {
        AtomicIOSession<K> atomicIOSession = this.IOSessionFactory.create(indexStorageManager, indexId, nodeFactory, keyValueSize);
        AbstractTreeNode<K> root = getRoot(atomicIOSession);
        return new CreateIndexOperation<>(dbConfig, atomicIOSession, kBinaryObjectFactory, vBinaryObjectFactory, keyValueSize).addIndex(root, identifier, value);
    }

    @Override
    public AbstractTreeNode<K> updateIndex(K identifier, V value) throws BTreeException, StorageException {
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
        return node;
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
    public boolean removeIndex(K identifier) throws BTreeException, StorageException {
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
}
