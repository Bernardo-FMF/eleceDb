package org.elece.query.comparator;

import org.elece.sql.parser.expression.internal.SqlNumberValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class NumberEqualityComparatorTest {
    @Test
    public void test_compareSameValue_expectEquality() {
        SqlNumberValue value = new SqlNumberValue(1);
        SqlNumberValue inputValue = new SqlNumberValue(1);

        NumberEqualityComparator comparator = new NumberEqualityComparator(value, true);

        Assertions.assertTrue(comparator.compare(inputValue));
    }

    @Test
    public void test_compareDifferentValues_expectEquality() {
        SqlNumberValue value = new SqlNumberValue(1);
        SqlNumberValue inputValue = new SqlNumberValue(2);

        NumberEqualityComparator comparator = new NumberEqualityComparator(value, true);

        Assertions.assertFalse(comparator.compare(inputValue));
    }

    @Test
    public void test_compareSameValue_expectInequality() {
        SqlNumberValue value = new SqlNumberValue(1);
        SqlNumberValue inputValue = new SqlNumberValue(1);

        NumberEqualityComparator comparator = new NumberEqualityComparator(value, false);

        Assertions.assertFalse(comparator.compare(inputValue));
    }

    @Test
    public void test_compareDifferentValues_expectInequality() {
        SqlNumberValue value = new SqlNumberValue(1);
        SqlNumberValue inputValue = new SqlNumberValue(2);

        NumberEqualityComparator comparator = new NumberEqualityComparator(value, false);

        Assertions.assertTrue(comparator.compare(inputValue));
    }

    @Test
    public void test_intersectSameValue_expectEquality() {
        SqlNumberValue value1 = new SqlNumberValue(1);
        SqlNumberValue value2 = new SqlNumberValue(1);

        NumberEqualityComparator comparator1 = new NumberEqualityComparator(value1, true);
        NumberEqualityComparator comparator2 = new NumberEqualityComparator(value2, true);

        Optional<ValueComparator<Integer>> intersection = comparator1.intersect(comparator2);
        Assertions.assertTrue(intersection.isPresent());

        intersection.ifPresent(valueComparator -> {
            Assertions.assertTrue(valueComparator.compare(value1));
            Assertions.assertTrue(valueComparator.compare(value2));
        });
    }

    @Test
    public void test_intersectSameValues_invalidIntersection() {
        SqlNumberValue value1 = new SqlNumberValue(1);
        SqlNumberValue value2 = new SqlNumberValue(1);

        NumberEqualityComparator comparator1 = new NumberEqualityComparator(value1, true);
        NumberEqualityComparator comparator2 = new NumberEqualityComparator(value2, false);

        Optional<ValueComparator<Integer>> intersection = comparator1.intersect(comparator2);
        Assertions.assertTrue(intersection.isEmpty());
    }

    @Test
    public void test_intersectDifferentValues_invalidIntersection() {
        SqlNumberValue value1 = new SqlNumberValue(1);
        SqlNumberValue value2 = new SqlNumberValue(2);

        NumberEqualityComparator comparator1 = new NumberEqualityComparator(value1, true);
        NumberEqualityComparator comparator2 = new NumberEqualityComparator(value2, true);

        Optional<ValueComparator<Integer>> intersection = comparator1.intersect(comparator2);
        Assertions.assertTrue(intersection.isEmpty());
    }

    @Test
    public void test_intersectSameValue_expectInequality() {
        SqlNumberValue value1 = new SqlNumberValue(1);
        SqlNumberValue value2 = new SqlNumberValue(1);

        NumberEqualityComparator comparator1 = new NumberEqualityComparator(value1, false);
        NumberEqualityComparator comparator2 = new NumberEqualityComparator(value2, false);

        Optional<ValueComparator<Integer>> intersection = comparator1.intersect(comparator2);
        Assertions.assertTrue(intersection.isPresent());

        intersection.ifPresent(valueComparator -> {
            Assertions.assertFalse(valueComparator.compare(value1));
            Assertions.assertFalse(valueComparator.compare(value2));
        });
    }
}