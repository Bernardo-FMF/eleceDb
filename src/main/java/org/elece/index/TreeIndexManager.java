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
import org.elece.storage.index.session.Session;
import org.elece.storage.index.session.factory.SessionFactory;
import org.elece.utils.BTreeUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class TreeIndexManager<K extends Comparable<K>, V> extends AbstractTreeIndexManager<K, V> {
    private final IndexStorageManager indexStorageManager;
    private final SessionFactory sessionFactory;
    private final DbConfig dbConfig;
    private final BinaryObjectFactory<K> kBinaryObjectFactory;
    private final BinaryObjectFactory<V> vBinaryObjectFactory;
    private final NodeFactory<K> nodeFactory;
    protected final KeyValueSize keyValueSize;

    public TreeIndexManager(int indexId, IndexStorageManager indexStorageManager, SessionFactory iOSessionFactory,
                            DbConfig dbConfig, BinaryObjectFactory<K> kBinaryObjectFactory,
                            BinaryObjectFactory<V> vBinaryObjectFactory, NodeFactory<K> nodeFactory) {
        super(indexId);
        this.indexStorageManager = indexStorageManager;
        this.sessionFactory = iOSessionFactory;
        this.dbConfig = dbConfig;
        this.kBinaryObjectFactory = kBinaryObjectFactory;
        this.vBinaryObjectFactory = vBinaryObjectFactory;
        this.nodeFactory = nodeFactory;
        this.keyValueSize = new KeyValueSize(kBinaryObjectFactory.size(), vBinaryObjectFactory.size());
    }

    @Override
    public void addIndex(K identifier, V value) throws BTreeException, StorageException, SerializationException,
                                                       InterruptedTaskException, FileChannelException {
        Session<K> session = this.sessionFactory.create(indexStorageManager, indexId, nodeFactory, keyValueSize);
        AbstractTreeNode<K> root = getRoot(session);
        new CreateIndexOperation<>(dbConfig, session, kBinaryObjectFactory, vBinaryObjectFactory, keyValueSize).addIndex(root, identifier, value);
    }

    @Override
    public void updateIndex(K identifier, V value) throws BTreeException, StorageException, SerializationException,
                                                          InterruptedTaskException, FileChannelException {
        Session<K> session = this.sessionFactory.create(indexStorageManager, indexId, nodeFactory, keyValueSize);

        int bTreeDegree = dbConfig.getBTreeDegree();
        LeafTreeNode<K, V> node = BTreeUtils.getResponsibleNode(indexStorageManager, getRoot(session), identifier, indexId, bTreeDegree, nodeFactory, vBinaryObjectFactory);
        List<K> keyList = node.getKeyList(bTreeDegree);
        if (!keyList.contains(identifier)) {
            throw new BTreeException(DbError.INDEX_NOT_FOUND_ERROR, "Failed to find indexed key");
        }

        node.setKeyValue(keyList.indexOf(identifier), new LeafTreeNode.KeyValue<>(identifier, value));
        session.update(node);
        session.commit();
    }

    @Override
    public Optional<V> getIndex(K identifier) throws BTreeException, StorageException, InterruptedTaskException,
                                                     FileChannelException {
        Session<K> session = this.sessionFactory.create(indexStorageManager, indexId, nodeFactory, keyValueSize);

        int bTreeDegree = dbConfig.getBTreeDegree();
        LeafTreeNode<K, V> baseTreeNode = BTreeUtils.getResponsibleNode(indexStorageManager, getRoot(session), identifier, indexId, bTreeDegree, nodeFactory, vBinaryObjectFactory);
        for (LeafTreeNode.KeyValue<K, V> entry : baseTreeNode.getKeyValueList(bTreeDegree)) {
            if (entry.key().compareTo(identifier) == 0) {
                return Optional.of(entry.value());
            }
        }

        return Optional.empty();
    }

    @Override
    public boolean removeIndex(K identifier) throws BTreeException, StorageException, SerializationException,
                                                    InterruptedTaskException, FileChannelException {
        Session<K> session = this.sessionFactory.create(indexStorageManager, indexId, nodeFactory, keyValueSize);
        AbstractTreeNode<K> root = getRoot(session);
        return new DeleteIndexOperation<>(dbConfig, session, vBinaryObjectFactory, nodeFactory, indexId).removeIndex(root, identifier);
    }

    @Override
    public void purgeIndex() throws InterruptedTaskException, StorageException, FileChannelException {
        if (this.indexStorageManager.supportsPurge()) {
            this.indexStorageManager.purgeIndex(indexId);
        }
    }

    @Override
    public LockableIterator<LeafTreeNode.KeyValue<K, V>> getSortedIterator() throws StorageException,
                                                                                    InterruptedTaskException,
                                                                                    FileChannelException {
        Session<K> session = this.sessionFactory.create(indexStorageManager, indexId, nodeFactory, keyValueSize);

        return new LockableIterator<>() {
            private int keyIndex = 0;
            LeafTreeNode<K, V> currentLeaf = getFarLeftLeaf(session, getRoot(session));

            @Override
            public void lock() {
                // default implementation does not lock/unlock
            }

            @Override
            public void unlock() {
                // default implementation does not lock/unlock
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
                        Optional<Pointer> nextSiblingPointer = currentLeaf.getNextSiblingPointer(dbConfig.getBTreeDegree());
                        if (nextSiblingPointer.isEmpty()) {
                            throw new StorageException(DbError.INTERNAL_STORAGE_ERROR, "Sibling pointer was deleted and points to empty memory location");
                        }
                        currentLeaf = (LeafTreeNode<K, V>) session.read(nextSiblingPointer.get());
                    } catch (StorageException | InterruptedTaskException | FileChannelException exception) {
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
    public Optional<K> getLastIndex() throws StorageException, InterruptedTaskException, FileChannelException {
        Session<K> session = this.sessionFactory.create(indexStorageManager, indexId, nodeFactory, keyValueSize);
        AbstractTreeNode<K> root = getRoot(session);
        LeafTreeNode<K, V> farRightLeaf = getFarRightLeaf(session, root);
        List<K> keyList = farRightLeaf.getKeyList(dbConfig.getBTreeDegree());
        return !keyList.isEmpty() ? Optional.of(keyList.getLast()) : Optional.empty();
    }

    private AbstractTreeNode<K> getRoot(Session<K> session) throws StorageException, InterruptedTaskException,
                                                                   FileChannelException {
        Optional<AbstractTreeNode<K>> optionalRoot = session.getRoot();
        if (optionalRoot.isPresent()) {
            return optionalRoot.get();
        }

        byte[] emptyNode = indexStorageManager.getEmptyNode(keyValueSize);
        LeafTreeNode<K, ?> leafTreeNode = (LeafTreeNode<K, ?>) nodeFactory.fromBytes(emptyNode, NodeType.LEAF);
        leafTreeNode.setAsRoot();

        NodeData nodeData = session.write(leafTreeNode);
        leafTreeNode.setPointer(nodeData.pointer());
        return leafTreeNode;
    }

    protected LeafTreeNode<K, V> getFarLeftLeaf(Session<K> session, AbstractTreeNode<K> root) throws StorageException,
                                                                                                     InterruptedTaskException,
                                                                                                     FileChannelException {
        if (root.isLeaf()) {
            return (LeafTreeNode<K, V>) root;
        }

        AbstractTreeNode<K> farLeftChild = root;

        while (!farLeftChild.isLeaf()) {
            farLeftChild = session.read(((InternalTreeNode<K>) farLeftChild).getChildAtIndex(0));
        }

        return (LeafTreeNode<K, V>) farLeftChild;
    }

    protected LeafTreeNode<K, V> getFarRightLeaf(Session<K> session, AbstractTreeNode<K> root) throws StorageException,
                                                                                                      InterruptedTaskException,
                                                                                                      FileChannelException {
        if (root.isLeaf()) {
            return (LeafTreeNode<K, V>) root;
        }

        AbstractTreeNode<K> farRightChild = root;

        while (!farRightChild.isLeaf()) {
            List<Pointer> childrenList = ((InternalTreeNode<K>) farRightChild).getChildrenList();
            farRightChild = session.read(childrenList.getLast());
        }

        return (LeafTreeNode<K, V>) farRightChild;
    }

    @Override
    public Iterator<V> getGreaterThan(K k, Set<K> kExclusions, Order order) throws StorageException, BTreeException,
                                                                                   InterruptedTaskException,
                                                                                   FileChannelException {
        return new QueryIterator(order, key -> key.compareTo(k) > 0, k, Symbol.GT, kExclusions);
    }

    @Override
    public Iterator<V> getGreaterThanEqual(K k, Set<K> kExclusions, Order order) throws StorageException,
                                                                                        BTreeException,
                                                                                        InterruptedTaskException,
                                                                                        FileChannelException {
        return new QueryIterator(order, key -> key.compareTo(k) >= 0, k, Symbol.GT_EQ, kExclusions);
    }

    @Override
    public Iterator<V> getLessThan(K k, Set<K> kExclusions, Order order) throws StorageException, BTreeException,
                                                                                InterruptedTaskException,
                                                                                FileChannelException {
        return new QueryIterator(order, key -> key.compareTo(k) < 0, k, Symbol.LT, kExclusions);
    }

    @Override
    public Iterator<V> getLessThanEqual(K k, Set<K> kExclusions, Order order) throws StorageException, BTreeException,
                                                                                     InterruptedTaskException,
                                                                                     FileChannelException {
        return new QueryIterator(order, key -> key.compareTo(k) <= 0, k, Symbol.LT_EQ, kExclusions);
    }

    @Override
    public Iterator<V> getBetweenRange(K k1, K k2, Set<K> kExclusions, Order order) throws StorageException,
                                                                                           BTreeException,
                                                                                           InterruptedTaskException,
                                                                                           FileChannelException {
        return new QueryIterator(order, key -> key.compareTo(k1) >= 0 && key.compareTo(k2) <= 0, k1, null, kExclusions);
    }

    private class QueryIterator implements Iterator<V> {
        private final Order order;
        private final Session<K> session;
        private final Function<K, Boolean> comparisonFunction;

        private final K key;
        private final Symbol operation;
        private final Set<K> kExclusions;

        private LeafTreeNode<K, V> targetTreeNode;
        private List<LeafTreeNode.KeyValue<K, V>> nodeKeyValueList;
        private Integer keyValueIndex;

        public QueryIterator(Order order, Function<K, Boolean> comparisonFunction, K key, Symbol operation,
                             Set<K> kExclusions) throws StorageException, BTreeException, InterruptedTaskException,
                                                        FileChannelException {
            this.order = order;
            this.comparisonFunction = comparisonFunction;
            this.key = key;
            this.operation = operation;
            this.kExclusions = kExclusions;
            this.session = sessionFactory.create(indexStorageManager, indexId, nodeFactory, keyValueSize);
            this.keyValueIndex = -1;

            locateInitialTargetNode();
        }

        private void locateInitialTargetNode() throws
                                               StorageException, BTreeException, InterruptedTaskException,
                                               FileChannelException {
            if (order == Order.ASC) {
                if (operation == Symbol.LT || operation == Symbol.LT_EQ) {
                    targetTreeNode = getFarLeftLeaf(session, getRoot(session));
                    nodeKeyValueList = targetTreeNode.getKeyValueList(dbConfig.getBTreeDegree());
                } else {
                    targetTreeNode = BTreeUtils.getResponsibleNode(indexStorageManager, getRoot(session), key, indexId, dbConfig.getBTreeDegree(), nodeFactory, vBinaryObjectFactory);
                    nodeKeyValueList = targetTreeNode.getKeyValueList(dbConfig.getBTreeDegree());

                    if (operation == Symbol.GT && nodeKeyValueList.getLast().key().compareTo(key) <= 0 && targetTreeNode.getNextSiblingPointer(dbConfig.getBTreeDegree()).isPresent()) {
                        targetTreeNode = (LeafTreeNode<K, V>) session.read(targetTreeNode.getNextSiblingPointer(dbConfig.getBTreeDegree()).get());
                        nodeKeyValueList = targetTreeNode.getKeyValueList(dbConfig.getBTreeDegree());
                    }
                }
                for (int keyIndex = 0; keyIndex < nodeKeyValueList.size(); keyIndex++) {
                    K currentKey = nodeKeyValueList.get(keyIndex).key();
                    if (Boolean.TRUE.equals(comparisonFunction.apply(currentKey)) && !kExclusions.contains(currentKey)) {
                        keyValueIndex = keyIndex;
                        break;
                    }
                }
            } else {
                if (operation == Symbol.GT || operation == Symbol.GT_EQ) {
                    targetTreeNode = getFarRightLeaf(session, getRoot(session));
                    nodeKeyValueList = targetTreeNode.getKeyValueList(dbConfig.getBTreeDegree());
                } else {
                    targetTreeNode = BTreeUtils.getResponsibleNode(indexStorageManager, getRoot(session), key, indexId, dbConfig.getBTreeDegree(), nodeFactory, vBinaryObjectFactory);
                    nodeKeyValueList = targetTreeNode.getKeyValueList(dbConfig.getBTreeDegree());

                    if (operation == Symbol.LT && nodeKeyValueList.getFirst().key().compareTo(key) >= 0 && targetTreeNode.getPreviousSiblingPointer(dbConfig.getBTreeDegree()).isPresent()) {
                        targetTreeNode = (LeafTreeNode<K, V>) session.read(targetTreeNode.getPreviousSiblingPointer(dbConfig.getBTreeDegree()).get());
                        nodeKeyValueList = targetTreeNode.getKeyValueList(dbConfig.getBTreeDegree());
                    }
                }
                for (int keyIndex = nodeKeyValueList.size() - 1; keyIndex >= 0; keyIndex--) {
                    K currentKey = nodeKeyValueList.get(keyIndex).key();
                    if (Boolean.TRUE.equals(comparisonFunction.apply(currentKey)) && !kExclusions.contains(currentKey)) {
                        keyValueIndex = keyIndex;
                        break;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            if (order == Order.ASC) {
                if (keyValueIndex == -1) {
                    return false;
                }

                if (keyValueIndex == nodeKeyValueList.size()) {
                    Optional<Pointer> nextSiblingPointer = targetTreeNode.getNextSiblingPointer(dbConfig.getBTreeDegree());
                    if (nextSiblingPointer.isEmpty()) {
                        return false;
                    }

                    try {
                        targetTreeNode = (LeafTreeNode<K, V>) session.read(nextSiblingPointer.get());
                    } catch (StorageException | InterruptedTaskException | FileChannelException exception) {
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
                        targetTreeNode = (LeafTreeNode<K, V>) session.read(previousSiblingPointer.get());
                    } catch (StorageException | InterruptedTaskException | FileChannelException exception) {
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
            if (order == Order.DESC) {
                keyValueIndex--;
            } else {
                keyValueIndex++;
            }
            return nextKey;
        }
    }
}
