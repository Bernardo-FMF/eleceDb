package org.elece.index.filter;

import org.elece.exception.BTreeException;
import org.elece.exception.SerializationException;
import org.elece.memory.data.BinaryObjectFactory;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.LongBinaryOperator;

/**
 * A space efficient, probabilistic set membership structure used to short circuit index lookups for keys that are
 * definitely absent. It answers {@link #mightContain(Comparable)} with either "definitely not present" (a guaranteed
 * answer) or "possibly present" (which may be a false positive). It never produces a false negative, provided every
 * inserted key is recorded and bits are never cleared, which is exactly the invariant its callers rely on.
 * <p>
 * Keys are turned into bytes through the same {@link BinaryObjectFactory} that the backing B+ tree uses, so the digest
 * matches the on disk representation of the key. A single 128-bit {@link Murmur3} digest is split into two halves and
 * combined via double hashing to simulate the configured number of hash functions.
 * <p>
 * The bit array is backed by an {@link AtomicLongArray} and mutated with atomic bitwise-or writes. Since bits only ever
 * transition from zero to one, concurrent readers can never observe a spuriously cleared bit, so membership checks stay
 * correct under concurrent insertion without locking.
 *
 * @param <K> the key type stored in the filter.
 */
public class BloomFilter<K extends Comparable<K>> {
    private static final int SEED = 0x9747b28c;
    private static final double LN2 = Math.log(2);
    private static final double DEFAULT_FALSE_POSITIVE_RATE = 0.01;
    private static final int MAX_BITS = 1 << 30;
    private static final LongBinaryOperator BITWISE_OR = (previous, mask) -> previous | mask;

    private final BinaryObjectFactory<K> keyFactory;
    private final AtomicLongArray words;
    private final int bitSize;
    private final int hashCount;

    public BloomFilter(int expectedInsertions, double falsePositiveRate, BinaryObjectFactory<K> keyFactory) {
        this.keyFactory = keyFactory;

        int insertions = Math.max(1, expectedInsertions);
        double rate = falsePositiveRate;
        if (Double.isNaN(rate) || rate <= 0.0 || rate >= 1.0) {
            rate = DEFAULT_FALSE_POSITIVE_RATE;
        }

        long optimalBits = (long) Math.ceil(-insertions * Math.log(rate) / (LN2 * LN2));
        this.bitSize = (int) Math.min(MAX_BITS, Math.max(Long.SIZE, optimalBits));
        this.hashCount = Math.max(1, (int) Math.round((double) bitSize / insertions * LN2));
        this.words = new AtomicLongArray((bitSize + Long.SIZE - 1) / Long.SIZE);
    }

    /**
     * Records a key in the filter. Propagates the serialization failures the backing factory would raise, so a key that
     * cannot be stored in the index cannot silently be dropped from the filter either.
     */
    public void add(K key) throws BTreeException, SerializationException {
        long[] hash = Murmur3.hash128(toBytes(key), SEED);
        for (int index = 0; index < hashCount; index++) {
            setBit(bitIndex(hash[0], hash[1], index));
        }
    }

    /**
     * Returns {@code false} only when the key is guaranteed to be absent; otherwise {@code true}. On any failure to hash
     * the key it conservatively returns {@code true}, deferring to the authoritative index so a hashing problem can
     * never turn into a false negative.
     */
    public boolean mightContain(K key) {
        byte[] bytes;
        try {
            bytes = toBytes(key);
        } catch (BTreeException | SerializationException exception) {
            return true;
        }

        long[] hash = Murmur3.hash128(bytes, SEED);
        for (int index = 0; index < hashCount; index++) {
            if (!getBit(bitIndex(hash[0], hash[1], index))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Resets every bit. Only safe to call when the backing index is known to be empty, otherwise the filter would start
     * reporting existing keys as absent.
     */
    public void clear() {
        for (int index = 0; index < words.length(); index++) {
            words.set(index, 0L);
        }
    }

    public int getBitSize() {
        return bitSize;
    }

    public int getHashCount() {
        return hashCount;
    }

    private int bitIndex(long hash1, long hash2, int hashFunction) {
        long combined = hash1 + (long) hashFunction * hash2;
        return (int) Long.remainderUnsigned(combined, bitSize);
    }

    private void setBit(int bit) {
        int wordIndex = bit >>> 6;
        long mask = 1L << (bit & 63);
        words.accumulateAndGet(wordIndex, mask, BITWISE_OR);
    }

    private boolean getBit(int bit) {
        int wordIndex = bit >>> 6;
        long mask = 1L << (bit & 63);
        return (words.get(wordIndex) & mask) != 0L;
    }

    private byte[] toBytes(K key) throws BTreeException, SerializationException {
        return keyFactory.create(key).getBytes();
    }
}
