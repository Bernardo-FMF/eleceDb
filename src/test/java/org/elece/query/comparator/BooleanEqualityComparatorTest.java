package org.elece.query.comparator;

import org.elece.sql.parser.expression.internal.SqlBoolValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class BooleanEqualityComparatorTest {
    @Test
    public void test_compareSameValue_expectEquality() {
        SqlBoolValue value = new SqlBoolValue(true);
        SqlBoolValue inputValue = new SqlBoolValue(true);

        BooleanEqualityComparator comparator = new BooleanEqualityComparator(value, true);

        Assertions.assertTrue(comparator.compare(inputValue));
    }

    @Test
    public void test_compareDifferentValues_expectEquality() {
        SqlBoolValue value = new SqlBoolValue(true);
        SqlBoolValue inputValue = new SqlBoolValue(false);

        BooleanEqualityComparator comparator = new BooleanEqualityComparator(value, true);

        Assertions.assertFalse(comparator.compare(inputValue));
    }

    @Test
    public void test_compareSameValue_expectInequality() {
        SqlBoolValue value = new SqlBoolValue(true);
        SqlBoolValue inputValue = new SqlBoolValue(true);

        BooleanEqualityComparator comparator = new BooleanEqualityComparator(value, false);

        Assertions.assertFalse(comparator.compare(inputValue));
    }

    @Test
    public void test_compareDifferentValues_expectInequality() {
        SqlBoolValue value = new SqlBoolValue(true);
        SqlBoolValue inputValue = new SqlBoolValue(false);

        BooleanEqualityComparator comparator = new BooleanEqualityComparator(value, false);

        Assertions.assertTrue(comparator.compare(inputValue));
    }

    @Test
    public void test_intersectSameValue_expectEquality() {
        SqlBoolValue value1 = new SqlBoolValue(true);
        SqlBoolValue value2 = new SqlBoolValue(true);

        BooleanEqualityComparator comparator1 = new BooleanEqualityComparator(value1, true);
        BooleanEqualityComparator comparator2 = new BooleanEqualityComparator(value2, true);

        Optional<ValueComparator<Boolean>> intersection = comparator1.intersect(comparator2);
        Assertions.assertTrue(intersection.isPresent());

        intersection.ifPresent(valueComparator -> {
            Assertions.assertTrue(valueComparator.compare(value1));
            Assertions.assertTrue(valueComparator.compare(value2));
        });
    }

    @Test
    public void test_intersectSameValues_invalidIntersection() {
        SqlBoolValue value1 = new SqlBoolValue(true);
        SqlBoolValue value2 = new SqlBoolValue(true);

        BooleanEqualityComparator comparator1 = new BooleanEqualityComparator(value1, true);
        BooleanEqualityComparator comparator2 = new BooleanEqualityComparator(value2, false);

        Optional<ValueComparator<Boolean>> intersection = comparator1.intersect(comparator2);
        Assertions.assertTrue(intersection.isEmpty());
    }

    @Test
    public void test_intersectDifferentValues_invalidIntersection() {
        SqlBoolValue value1 = new SqlBoolValue(true);
        SqlBoolValue value2 = new SqlBoolValue(false);

        BooleanEqualityComparator comparator1 = new BooleanEqualityComparator(value1, true);
        BooleanEqualityComparator comparator2 = new BooleanEqualityComparator(value2, true);

        Optional<ValueComparator<Boolean>> intersection = comparator1.intersect(comparator2);
        Assertions.assertTrue(intersection.isEmpty());
    }

    @Test
    public void test_intersectSameValue_expectInequality() {
        SqlBoolValue value1 = new SqlBoolValue(true);
        SqlBoolValue value2 = new SqlBoolValue(true);

        BooleanEqualityComparator comparator1 = new BooleanEqualityComparator(value1, false);
        BooleanEqualityComparator comparator2 = new BooleanEqualityComparator(value2, false);

        Optional<ValueComparator<Boolean>> intersection = comparator1.intersect(comparator2);
        Assertions.assertTrue(intersection.isPresent());

        intersection.ifPresent(valueComparator -> {
            Assertions.assertFalse(valueComparator.compare(value1));
            Assertions.assertFalse(valueComparator.compare(value2));
        });
    }
}