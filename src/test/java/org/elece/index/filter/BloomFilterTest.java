package org.elece.index.filter;

import org.elece.db.schema.model.builder.ColumnBuilder;
import org.elece.exception.BTreeException;
import org.elece.exception.SerializationException;
import org.elece.memory.data.BinaryObjectFactory;
import org.elece.serializer.IntegerSerializer;
import org.elece.sql.parser.expression.internal.SqlType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BloomFilterTest {
    private static BinaryObjectFactory<Integer> integerFactory() {
        return new IntegerSerializer().getBinaryObjectFactory(ColumnBuilder.builder().setSqlType(SqlType.intType).build());
    }

    @Test
    void test_neverReportsFalseNegative() throws BTreeException, SerializationException {
        BloomFilter<Integer> filter = new BloomFilter<>(10_000, 0.01, integerFactory());

        for (int key = 1; key <= 10_000; key++) {
            filter.add(key);
        }

        for (int key = 1; key <= 10_000; key++) {
            Assertions.assertTrue(filter.mightContain(key), "present key wrongly reported absent: " + key);
        }
    }

    @Test
    void test_falsePositiveRateWithinBound() throws BTreeException, SerializationException {
        int insertions = 10_000;
        double rate = 0.01;
        BloomFilter<Integer> filter = new BloomFilter<>(insertions, rate, integerFactory());

        for (int key = 1; key <= insertions; key++) {
            filter.add(key);
        }

        int probes = 10_000;
        int falsePositives = 0;
        for (int key = insertions + 1; key <= insertions + probes; key++) {
            if (filter.mightContain(key)) {
                falsePositives++;
            }
        }

        double observed = (double) falsePositives / probes;
        // Allow generous headroom over the configured rate to absorb hashing variance without flaking.
        Assertions.assertTrue(observed < rate * 5, "observed false positive rate too high: " + observed);
    }

    @Test
    void test_clearResetsMembership() throws BTreeException, SerializationException {
        BloomFilter<Integer> filter = new BloomFilter<>(1_000, 0.01, integerFactory());

        for (int key = 1; key <= 1_000; key++) {
            filter.add(key);
        }
        filter.clear();

        int positives = 0;
        for (int key = 1; key <= 1_000; key++) {
            if (filter.mightContain(key)) {
                positives++;
            }
        }
        Assertions.assertEquals(0, positives, "cleared filter still reports members");
    }

    @Test
    void test_sizingFollowsConfiguredParameters() {
        BloomFilter<Integer> filter = new BloomFilter<>(10_000, 0.01, integerFactory());

        // The optimal sizing for n=10000, p=0.01 is ~95851 bits and 7 hash functions.
        Assertions.assertTrue(filter.getBitSize() >= 90_000 && filter.getBitSize() <= 100_000,
                "unexpected bit size: " + filter.getBitSize());
        Assertions.assertEquals(7, filter.getHashCount());
    }
}
