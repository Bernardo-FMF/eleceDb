package org.elece.query.comparator;

import org.elece.sql.parser.expression.internal.SqlStringValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class StringEqualityComparatorTest {
    @Test
    void test_compareSameValue_expectEquality() {
        SqlStringValue value = new SqlStringValue("user1");
        SqlStringValue inputValue = new SqlStringValue("user1");

        StringEqualityComparator comparator = new StringEqualityComparator(value, true);

        Assertions.assertTrue(comparator.compare(inputValue));
    }

    @Test
    void test_compareDifferentValues_expectEquality() {
        SqlStringValue value = new SqlStringValue("user1");
        SqlStringValue inputValue = new SqlStringValue("user2");

        StringEqualityComparator comparator = new StringEqualityComparator(value, true);

        Assertions.assertFalse(comparator.compare(inputValue));
    }

    @Test
    void test_compareSameValue_expectInequality() {
        SqlStringValue value = new SqlStringValue("user1");
        SqlStringValue inputValue = new SqlStringValue("user1");

        StringEqualityComparator comparator = new StringEqualityComparator(value, false);

        Assertions.assertFalse(comparator.compare(inputValue));
    }

    @Test
    void test_compareDifferentValues_expectInequality() {
        SqlStringValue value = new SqlStringValue("user1");
        SqlStringValue inputValue = new SqlStringValue("user2");

        StringEqualityComparator comparator = new StringEqualityComparator(value, false);

        Assertions.assertTrue(comparator.compare(inputValue));
    }

    @Test
    void test_intersectSameValue_expectEquality() {
        SqlStringValue value1 = new SqlStringValue("user1");
        SqlStringValue value2 = new SqlStringValue("user1");

        StringEqualityComparator comparator1 = new StringEqualityComparator(value1, true);
        StringEqualityComparator comparator2 = new StringEqualityComparator(value2, true);

        Optional<ValueComparator<String>> intersection = comparator1.intersect(comparator2);
        Assertions.assertTrue(intersection.isPresent());

        intersection.ifPresent(valueComparator -> {
            Assertions.assertTrue(valueComparator.compare(value1));
            Assertions.assertTrue(valueComparator.compare(value2));
        });
    }

    @Test
    void test_intersectSameValues_invalidIntersection() {
        SqlStringValue value1 = new SqlStringValue("user1");
        SqlStringValue value2 = new SqlStringValue("user1");

        StringEqualityComparator comparator1 = new StringEqualityComparator(value1, true);
        StringEqualityComparator comparator2 = new StringEqualityComparator(value2, false);

        Optional<ValueComparator<String>> intersection = comparator1.intersect(comparator2);
        Assertions.assertTrue(intersection.isEmpty());
    }

    @Test
    void test_intersectDifferentValues_invalidIntersection() {
        SqlStringValue value1 = new SqlStringValue("user1");
        SqlStringValue value2 = new SqlStringValue("user2");

        StringEqualityComparator comparator1 = new StringEqualityComparator(value1, true);
        StringEqualityComparator comparator2 = new StringEqualityComparator(value2, true);

        Optional<ValueComparator<String>> intersection = comparator1.intersect(comparator2);
        Assertions.assertTrue(intersection.isEmpty());
    }

    @Test
    void test_intersectSameValue_expectInequality() {
        SqlStringValue value1 = new SqlStringValue("user1");
        SqlStringValue value2 = new SqlStringValue("user1");

        StringEqualityComparator comparator1 = new StringEqualityComparator(value1, false);
        StringEqualityComparator comparator2 = new StringEqualityComparator(value2, false);

        Optional<ValueComparator<String>> intersection = comparator1.intersect(comparator2);
        Assertions.assertTrue(intersection.isPresent());

        intersection.ifPresent(valueComparator -> {
            Assertions.assertFalse(valueComparator.compare(value1));
            Assertions.assertFalse(valueComparator.compare(value2));
        });
    }
}
