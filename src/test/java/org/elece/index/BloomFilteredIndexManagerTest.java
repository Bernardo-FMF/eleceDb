package org.elece.index;

import org.elece.db.schema.model.builder.ColumnBuilder;
import org.elece.index.filter.BloomFilter;
import org.elece.memory.data.BinaryObjectFactory;
import org.elece.memory.tree.node.LeafTreeNode;
import org.elece.serializer.IntegerSerializer;
import org.elece.sql.parser.expression.internal.Order;
import org.elece.sql.parser.expression.internal.SqlType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

class BloomFilteredIndexManagerTest {
    private static BinaryObjectFactory<Integer> integerFactory() {
        return new IntegerSerializer().getBinaryObjectFactory(ColumnBuilder.builder().setSqlType(SqlType.intType).build());
    }

    private static BloomFilteredIndexManager<Integer, Integer> wrap(CountingIndexManager delegate) {
        return new BloomFilteredIndexManager<>(1, delegate, new BloomFilter<>(1_000, 0.01, integerFactory()));
    }

    @Test
    void test_shortCircuitsAbsentKeys() throws Exception {
        CountingIndexManager delegate = new CountingIndexManager();
        BloomFilteredIndexManager<Integer, Integer> manager = wrap(delegate);

        for (int key = 1; key <= 500; key++) {
            manager.addIndex(key, key);
        }
        delegate.getIndexCalls = 0;

        int probes = 2_000;
        for (int key = 10_000; key < 10_000 + probes; key++) {
            Assertions.assertTrue(manager.getIndex(key).isEmpty());
        }

        // Absent keys should mostly be resolved by the filter without ever reaching the delegate.
        Assertions.assertTrue(delegate.getIndexCalls < probes / 2,
                "expected the filter to short circuit most absent lookups, delegate calls: " + delegate.getIndexCalls);
    }

    @Test
    void test_presentKeysAlwaysReachDelegate() throws Exception {
        CountingIndexManager delegate = new CountingIndexManager();
        BloomFilteredIndexManager<Integer, Integer> manager = wrap(delegate);

        for (int key = 1; key <= 500; key++) {
            manager.addIndex(key, key * 10);
        }

        for (int key = 1; key <= 500; key++) {
            Optional<Integer> value = manager.getIndex(key);
            Assertions.assertTrue(value.isPresent(), "present key skipped: " + key);
            Assertions.assertEquals(key * 10, value.get());
        }
    }

    @Test
    void test_removalDoesNotHideOtherKeys() throws Exception {
        CountingIndexManager delegate = new CountingIndexManager();
        BloomFilteredIndexManager<Integer, Integer> manager = wrap(delegate);

        for (int key = 1; key <= 10; key++) {
            manager.addIndex(key, key);
        }

        Assertions.assertTrue(manager.removeIndex(5));
        Assertions.assertTrue(manager.getIndex(5).isEmpty());

        for (int key = 1; key <= 10; key++) {
            if (key == 5) {
                continue;
            }
            Assertions.assertTrue(manager.getIndex(key).isPresent(), "removal hid an unrelated key: " + key);
        }
    }

    @Test
    void test_lazilyPopulatesFromExistingIndex() throws Exception {
        CountingIndexManager delegate = new CountingIndexManager();
        // Simulate rows persisted before this manager existed, e.g. an index reopened after a restart.
        for (int key = 1; key <= 200; key++) {
            delegate.addIndex(key, key);
        }

        BloomFilteredIndexManager<Integer, Integer> manager = wrap(delegate);

        for (int key = 1; key <= 200; key++) {
            Assertions.assertTrue(manager.getIndex(key).isPresent(), "pre-existing key not found: " + key);
        }
    }

    @Test
    void test_purgeClearsFilterAndDelegate() throws Exception {
        CountingIndexManager delegate = new CountingIndexManager();
        BloomFilteredIndexManager<Integer, Integer> manager = wrap(delegate);

        for (int key = 1; key <= 50; key++) {
            manager.addIndex(key, key);
        }

        manager.purgeIndex();

        for (int key = 1; key <= 50; key++) {
            Assertions.assertTrue(manager.getIndex(key).isEmpty());
        }
    }

    /**
     * Minimal in-memory {@link IndexManager} that records how many times {@link #getIndex(Integer)} is invoked, so tests
     * can assert whether the filter short circuited a lookup before it reached the backing store.
     */
    private static final class CountingIndexManager extends AbstractTreeIndexManager<Integer, Integer> {
        private final TreeMap<Integer, Integer> store = new TreeMap<>();
        private int getIndexCalls = 0;

        private CountingIndexManager() {
            super(1);
        }

        @Override
        public void addIndex(Integer identifier, Integer value) {
            store.put(identifier, value);
        }

        @Override
        public void updateIndex(Integer identifier, Integer value) {
            store.put(identifier, value);
        }

        @Override
        public Optional<Integer> getIndex(Integer identifier) {
            getIndexCalls++;
            return Optional.ofNullable(store.get(identifier));
        }

        @Override
        public boolean removeIndex(Integer identifier) {
            return store.remove(identifier) != null;
        }

        @Override
        public void purgeIndex() {
            store.clear();
        }

        @Override
        public LockableIterator<LeafTreeNode.KeyValue<Integer, Integer>> getSortedIterator() {
            Iterator<Map.Entry<Integer, Integer>> entries = store.entrySet().iterator();
            return new LockableIterator<>() {
                @Override
                public void lock() {
                    // no-op
                }

                @Override
                public void unlock() {
                    // no-op
                }

                @Override
                public boolean hasNext() {
                    return entries.hasNext();
                }

                @Override
                public LeafTreeNode.KeyValue<Integer, Integer> next() {
                    Map.Entry<Integer, Integer> entry = entries.next();
                    return new LeafTreeNode.KeyValue<>(entry.getKey(), entry.getValue());
                }
            };
        }

        @Override
        public Optional<Integer> getLastIndex() {
            return store.isEmpty() ? Optional.empty() : Optional.of(store.lastKey());
        }

        @Override
        public Iterator<Integer> getGreaterThan(Integer k, Set<Integer> kExclusions, Order order) {
            return Collections.emptyIterator();
        }

        @Override
        public Iterator<Integer> getGreaterThanEqual(Integer k, Set<Integer> kExclusions, Order order) {
            return Collections.emptyIterator();
        }

        @Override
        public Iterator<Integer> getLessThan(Integer k, Set<Integer> kExclusions, Order order) {
            return Collections.emptyIterator();
        }

        @Override
        public Iterator<Integer> getLessThanEqual(Integer k, Set<Integer> kExclusions, Order order) {
            return Collections.emptyIterator();
        }

        @Override
        public Iterator<Integer> getBetweenRange(Integer k1, Integer k2, Set<Integer> kExclusions, Order order) {
            return Collections.emptyIterator();
        }
    }
}
