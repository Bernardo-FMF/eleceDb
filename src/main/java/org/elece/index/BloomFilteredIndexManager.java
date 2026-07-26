package org.elece.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elece.exception.*;
import org.elece.index.filter.BloomFilter;
import org.elece.memory.tree.node.LeafTreeNode;
import org.elece.sql.parser.expression.internal.Order;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

/**
 * Wraps an {@link IndexManager} with a {@link BloomFilter} so that equality lookups for keys that are definitely absent
 * can be answered without traversing the underlying B+ tree, avoiding the page reads such a traversal would incur.
 * <p>
 * The filter is only ever consulted as a negative check: if it reports a key as absent the lookup returns empty
 * immediately, otherwise the call falls through to the delegate exactly as before. False positives are therefore
 * harmless (a wasted tree lookup that finds nothing) while false negatives are prevented by construction:
 * <ul>
 *     <li>insertions record the key in the filter before it reaches the tree, so the filter is never behind the tree;</li>
 *     <li>removals deliberately leave the filter untouched, since clearing a bit could evict other keys that share it
 *     (the cost is a gradually rising false positive rate as deleted keys accumulate, reset only on a purge);</li>
 *     <li>on first read the filter is lazily populated from the keys already persisted in the index, so data that
 *     survived a restart is accounted for before the filter is trusted.</li>
 * </ul>
 * Range queries cannot benefit from a membership filter and are delegated unchanged.
 *
 * @param <K> the key type of the index.
 * @param <V> the value type of the index.
 */
public class BloomFilteredIndexManager<K extends Comparable<K>, V> extends AbstractTreeIndexManager<K, V> {
    private static final Logger logger = LogManager.getLogger(BloomFilteredIndexManager.class);

    private final IndexManager<K, V> delegate;
    private final BloomFilter<K> bloomFilter;
    private final Object populationLock = new Object();

    private volatile boolean populated = false;
    private volatile boolean bypass = false;

    public BloomFilteredIndexManager(int indexId, IndexManager<K, V> delegate, BloomFilter<K> bloomFilter) {
        super(indexId);
        this.delegate = delegate;
        this.bloomFilter = bloomFilter;
    }

    @Override
    public void addIndex(K identifier, V value) throws BTreeException, StorageException, SerializationException,
            InterruptedTaskException, FileChannelException {
        // Record in the filter first: if the delegate rejects the key the filter merely gains a false positive (safe),
        // whereas recording it after a successful insert risks a false negative if the filter update were to fail.
        bloomFilter.add(identifier);
        delegate.addIndex(identifier, value);
    }

    @Override
    public void updateIndex(K identifier, V value) throws BTreeException, StorageException, SerializationException,
            InterruptedTaskException, FileChannelException {
        delegate.updateIndex(identifier, value);
    }

    @Override
    public Optional<V> getIndex(K identifier) throws BTreeException, StorageException, InterruptedTaskException,
            FileChannelException {
        ensurePopulated();
        if (!bypass && !bloomFilter.mightContain(identifier)) {
            return Optional.empty();
        }
        return delegate.getIndex(identifier);
    }

    @Override
    public boolean removeIndex(K identifier) throws BTreeException, StorageException, SerializationException,
            InterruptedTaskException, FileChannelException {
        // Intentionally not removed from the filter, to preserve the no false negative invariant.
        return delegate.removeIndex(identifier);
    }

    @Override
    public void purgeIndex() throws InterruptedTaskException, StorageException, FileChannelException {
        delegate.purgeIndex();
        bloomFilter.clear();
    }

    @Override
    public LockableIterator<LeafTreeNode.KeyValue<K, V>> getSortedIterator() throws StorageException,
            InterruptedTaskException,
            FileChannelException {
        return delegate.getSortedIterator();
    }

    @Override
    public Optional<K> getLastIndex() throws StorageException, InterruptedTaskException, FileChannelException {
        return delegate.getLastIndex();
    }

    @Override
    public Iterator<V> getGreaterThan(K k, Set<K> kExclusions, Order order) throws StorageException, BTreeException,
            InterruptedTaskException,
            FileChannelException {
        return delegate.getGreaterThan(k, kExclusions, order);
    }

    @Override
    public Iterator<V> getGreaterThanEqual(K k, Set<K> kExclusions, Order order) throws StorageException,
            BTreeException,
            InterruptedTaskException,
            FileChannelException {
        return delegate.getGreaterThanEqual(k, kExclusions, order);
    }

    @Override
    public Iterator<V> getLessThan(K k, Set<K> kExclusions, Order order) throws StorageException, BTreeException,
            InterruptedTaskException,
            FileChannelException {
        return delegate.getLessThan(k, kExclusions, order);
    }

    @Override
    public Iterator<V> getLessThanEqual(K k, Set<K> kExclusions, Order order) throws StorageException, BTreeException,
            InterruptedTaskException,
            FileChannelException {
        return delegate.getLessThanEqual(k, kExclusions, order);
    }

    @Override
    public Iterator<V> getBetweenRange(K k1, K k2, Set<K> kExclusions, Order order) throws StorageException,
            BTreeException,
            InterruptedTaskException,
            FileChannelException {
        return delegate.getBetweenRange(k1, k2, kExclusions, order);
    }

    /**
     * Populates the filter, once, from the keys already stored in the delegate index. This captures rows that were
     * persisted before this manager was created (for instance across a restart) so the filter is never consulted while
     * it is behind the index. If the keys cannot be read the filter is disabled ({@link #bypass}), degrading to plain
     * delegation rather than risking a false negative.
     */
    private void ensurePopulated() throws StorageException, InterruptedTaskException, FileChannelException {
        if (populated || bypass) {
            return;
        }
        synchronized (populationLock) {
            if (populated || bypass) {
                return;
            }

            LockableIterator<LeafTreeNode.KeyValue<K, V>> iterator = delegate.getSortedIterator();
            try {
                iterator.lock();
                while (iterator.hasNext()) {
                    bloomFilter.add(iterator.next().key());
                }
                populated = true;
            } catch (BTreeException | SerializationException exception) {
                bypass = true;
                logger.warn("Failed to populate bloom filter for index {}, disabling it", getIndexId(), exception);
            } finally {
                iterator.unlock();
            }
        }
    }
}
