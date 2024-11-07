package org.elece.index;

import org.elece.config.DbConfig;
import org.elece.exception.*;
import org.elece.memory.KeyValueSize;
import org.elece.memory.Pointer;
import org.elece.memory.data.BinaryObjectFactory;
import org.elece.memory.tree.node.*;
import org.elece.memory.tree.operation.CreateIndexOperation;
import org.elece.memory.tree.operation.DeleteIndexOperation;
import org.elece.sql.parser.expression.internal.Order;
import org.elece.sql.token.model.type.Symbol;
import org.elece.storage.index.IndexStorageManager;
import org.elece.storage.index.NodeData;
import org.elece.storage.index.session.AtomicIOSession;
import org.elece.storage.index.session.factory.IOSessionFactory;
import org.elece.utils.BTreeUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class TreeIndexManager<K extends Comparable<K>, V> extends AbstractTreeIndexManager<K, V> {
    private final IndexStorageManager indexStorageManager;
    private final IOSessionFactory IOSessionFactory;
    private final DbConfig dbConfig;
    private final BinaryObjectFactory<K> kBinaryObjectFactory;
    private final BinaryObjectFactory<V> vBinaryObjectFactory;
    private final NodeFactory<K> nodeFactory;
    protected final KeyValueSize keyValueSize;

    public TreeIndexManager(int indexId, IndexStorageManager indexStorageManager, IOSessionFactory iOSessionFactory,
                            DbConfig dbConfig, BinaryObjectFactory<K> kBinaryObjectFactory,
                            BinaryObjectFactory<V> vBinaryObjectFactory, NodeFactory<K> nodeFactory) {
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
            throw new BTreeException(DbError.INDEX_NOT_FOUND_ERROR, "Failed to find indexed key");
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
            LeafTreeNode<K, V> currentLeaf = getFarLeftLeaf(atomicIOSession, getRoot(atomicIOSession));

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
                        throw new RuntimeDbException(exception.getDbError(), exception.getMessage());
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
        AtomicIOSession<K> atomicIOSession = this.IOSessionFactory.create(indexStorageManager, indexId, nodeFactory, keyValueSize);
        AbstractTreeNode<K> root = getRoot(atomicIOSession);
        LeafTreeNode<K, V> farRightLeaf = getFarRightLeaf(atomicIOSession, root);
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

    protected LeafTreeNode<K, V> getFarLeftLeaf(AtomicIOSession<K> atomicIOSession, AbstractTreeNode<K> root) throws
                                                                                                              StorageException {
        if (root.isLeaf()) {
            return (LeafTreeNode<K, V>) root;
        }

        AbstractTreeNode<K> farLeftChild = root;

        while (!farLeftChild.isLeaf()) {
            farLeftChild = atomicIOSession.read(((InternalTreeNode<K>) farLeftChild).getChildAtIndex(0));
        }

        return (LeafTreeNode<K, V>) farLeftChild;
    }

    protected LeafTreeNode<K, V> getFarRightLeaf(AtomicIOSession<K> atomicIOSession, AbstractTreeNode<K> root) throws
                                                                                                               StorageException {
        if (root.isLeaf()) {
            return (LeafTreeNode<K, V>) root;
        }

        AbstractTreeNode<K> farRightChild = root;

        while (!farRightChild.isLeaf()) {
            List<Pointer> childrenList = ((InternalTreeNode<K>) farRightChild).getChildrenList();
            farRightChild = atomicIOSession.read(childrenList.getLast());
        }

        return (LeafTreeNode<K, V>) farRightChild;
    }

    @Override
    public Iterator<V> getGreaterThan(K k, Set<K> kExclusions, Order order) throws StorageException, BTreeException {
        return new QueryIterator(order, (key) -> key.compareTo(k) > 0, k, Symbol.Gt, kExclusions);
    }

    @Override
    public Iterator<V> getGreaterThanEqual(K k, Set<K> kExclusions, Order order) throws StorageException,
                                                                                        BTreeException {
        return new QueryIterator(order, (key) -> key.compareTo(k) >= 0, k, Symbol.GtEq, kExclusions);
    }

    @Override
    public Iterator<V> getLessThan(K k, Set<K> kExclusions, Order order) throws StorageException, BTreeException {
        return new QueryIterator(order, (key) -> key.compareTo(k) < 0, k, Symbol.Lt, kExclusions);
    }

    @Override
    public Iterator<V> getLessThanEqual(K k, Set<K> kExclusions, Order order) throws StorageException, BTreeException {
        return new QueryIterator(order, (key) -> key.compareTo(k) <= 0, k, Symbol.LtEq, kExclusions);
    }

    @Override
    public Iterator<V> getBetweenRange(K k1, K k2, Set<K> kExclusions, Order order) throws StorageException,
                                                                                           BTreeException {
        return new QueryIterator(order, (key) -> key.compareTo(k1) >= 0 && key.compareTo(k2) <= 0, k1, null, kExclusions);
    }

    private class QueryIterator implements Iterator<V> {
        private final Order order;
        private final AtomicIOSession<K> atomicIOSession;
        private final Function<K, Boolean> comparisonFunction;

        private final K key;
        private final Symbol operation;
        private final Set<K> kExclusions;

        private LeafTreeNode<K, V> targetTreeNode;
        private List<LeafTreeNode.KeyValue<K, V>> nodeKeyValueList;
        private Integer keyValueIndex;

        public QueryIterator(Order order, Function<K, Boolean> comparisonFunction, K key, Symbol operation,
                             Set<K> kExclusions) throws StorageException, BTreeException {
            this.order = order;
            this.comparisonFunction = comparisonFunction;
            this.key = key;
            this.operation = operation;
            this.kExclusions = kExclusions;
            this.atomicIOSession = IOSessionFactory.create(indexStorageManager, indexId, nodeFactory, keyValueSize);
            this.keyValueIndex = -1;

            locateInitialTargetNode();
        }

        private void locateInitialTargetNode() throws
                                               StorageException, BTreeException {
            if (order == Order.Asc) {
                if (operation == Symbol.Lt || operation == Symbol.LtEq) {
                    targetTreeNode = getFarLeftLeaf(atomicIOSession, getRoot(atomicIOSession));
                    nodeKeyValueList = targetTreeNode.getKeyValueList(dbConfig.getBTreeDegree());
                } else {
                    targetTreeNode = BTreeUtils.getResponsibleNode(indexStorageManager, getRoot(atomicIOSession), key, indexId, dbConfig.getBTreeDegree(), nodeFactory, vBinaryObjectFactory);
                    nodeKeyValueList = targetTreeNode.getKeyValueList(dbConfig.getBTreeDegree());

                    if (operation == Symbol.Gt && nodeKeyValueList.getLast().key().compareTo(key) <= 0 && targetTreeNode.getNextSiblingPointer(dbConfig.getBTreeDegree()).isPresent()) {
                        targetTreeNode = (LeafTreeNode<K, V>) atomicIOSession.read(targetTreeNode.getNextSiblingPointer(dbConfig.getBTreeDegree()).get());
                        nodeKeyValueList = targetTreeNode.getKeyValueList(dbConfig.getBTreeDegree());
                    }
                }
                for (int keyIndex = 0; keyIndex < nodeKeyValueList.size(); keyIndex++) {
                    K currentKey = nodeKeyValueList.get(keyIndex).key();
                    if (comparisonFunction.apply(currentKey) && !kExclusions.contains(currentKey)) {
                        keyValueIndex = keyIndex;
                        break;
                    }
                }
            } else {
                if (operation == Symbol.Gt || operation == Symbol.GtEq) {
                    targetTreeNode = getFarRightLeaf(atomicIOSession, getRoot(atomicIOSession));
                    nodeKeyValueList = targetTreeNode.getKeyValueList(dbConfig.getBTreeDegree());
                } else {
                    targetTreeNode = BTreeUtils.getResponsibleNode(indexStorageManager, getRoot(atomicIOSession), key, indexId, dbConfig.getBTreeDegree(), nodeFactory, vBinaryObjectFactory);
                    nodeKeyValueList = targetTreeNode.getKeyValueList(dbConfig.getBTreeDegree());

                    if (operation == Symbol.Lt && nodeKeyValueList.getFirst().key().compareTo(key) >= 0 && targetTreeNode.getPreviousSiblingPointer(dbConfig.getBTreeDegree()).isPresent()) {
                        targetTreeNode = (LeafTreeNode<K, V>) atomicIOSession.read(targetTreeNode.getPreviousSiblingPointer(dbConfig.getBTreeDegree()).get());
                        nodeKeyValueList = targetTreeNode.getKeyValueList(dbConfig.getBTreeDegree());
                    }
                }
                for (int keyIndex = nodeKeyValueList.size() - 1; keyIndex >= 0; keyIndex--) {
                    K currentKey = nodeKeyValueList.get(keyIndex).key();
                    if (comparisonFunction.apply(currentKey) && !kExclusions.contains(currentKey)) {
                        keyValueIndex = keyIndex;
                        break;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            if (order == Order.Asc) {
                if (keyValueIndex == -1) {
                    return false;
                }

                if (keyValueIndex == nodeKeyValueList.size()) {
                    Optional<Pointer> nextSiblingPointer = targetTreeNode.getNextSiblingPointer(dbConfig.getBTreeDegree());
                    if (nextSiblingPointer.isEmpty()) {
                        return false;
                    }

                    try {
                        targetTreeNode = (LeafTreeNode<K, V>) atomicIOSession.read(nextSiblingPointer.get());
                    } catch (StorageException exception) {
                        return false;
                    }
                    nodeKeyValueList = targetTreeNode.getKeyValueList(dbConfig.getBTreeDegree());
                    keyValueIndex = 0;
                }
            } else {
                if (keyValueIndex == -1) {
                    Optional<Pointer> previousSiblingPointer = targetTreeNode.getPreviousSiblingPointer(dbConfig.getBTreeDegree());
                    if (previousSiblingPointer.isEmpty()) {
                        return false;
                    }

                    try {
                        targetTreeNode = (LeafTreeNode<K, V>) atomicIOSession.read(previousSiblingPointer.get());
                    } catch (StorageException e) {
                        return false;
                    }
                    nodeKeyValueList = targetTreeNode.getKeyValueList(dbConfig.getBTreeDegree());
                    keyValueIndex = nodeKeyValueList.size() - 1;
                }
            }

            return comparisonFunction.apply(nodeKeyValueList.get(keyValueIndex).key());
        }

        @Override
        public V next() {
            V nextKey = nodeKeyValueList.get(keyValueIndex).value();
            if (order == Order.Desc) {
                keyValueIndex--;
            } else {
                keyValueIndex++;
            }
            return nextKey;
        }
    }
}
