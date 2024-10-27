package org.elece.query.comparator;

import org.elece.sql.parser.expression.internal.SqlNumberValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class NumberRangeComparatorTest {
    @Test
    public void test_compareValueWithinRange_1() {
        SqlNumberValue leftBoundary = new SqlNumberValue(1);
        SqlNumberValue rightBoundary = new SqlNumberValue(4);

        NumberRangeComparator numberRangeComparator = new NumberRangeComparator(leftBoundary, rightBoundary,
                NumberRangeComparator.InclusionType.Included, NumberRangeComparator.InclusionType.Included);

        Assertions.assertTrue(numberRangeComparator.compare(new SqlNumberValue(1)));
        Assertions.assertTrue(numberRangeComparator.compare(new SqlNumberValue(4)));
        Assertions.assertFalse(numberRangeComparator.compare(new SqlNumberValue(5)));
        Assertions.assertFalse(numberRangeComparator.compare(new SqlNumberValue(0)));
        Assertions.assertTrue(numberRangeComparator.compare(new SqlNumberValue(2)));
    }

    @Test
    public void test_compareValueWithinRange_2() {
        SqlNumberValue leftBoundary = new SqlNumberValue(1);
        SqlNumberValue rightBoundary = new SqlNumberValue(4);

        NumberRangeComparator numberRangeComparator = new NumberRangeComparator(leftBoundary, rightBoundary,
                NumberRangeComparator.InclusionType.Excluded, NumberRangeComparator.InclusionType.Excluded);

        Assertions.assertFalse(numberRangeComparator.compare(new SqlNumberValue(1)));
        Assertions.assertFalse(numberRangeComparator.compare(new SqlNumberValue(4)));
        Assertions.assertFalse(numberRangeComparator.compare(new SqlNumberValue(5)));
        Assertions.assertFalse(numberRangeComparator.compare(new SqlNumberValue(0)));
        Assertions.assertTrue(numberRangeComparator.compare(new SqlNumberValue(2)));
    }

    @Test
    public void test_compareValueWithinRange_3() {
        SqlNumberValue leftBoundary = new SqlNumberValue(1);

        NumberRangeComparator numberRangeComparator = new NumberRangeComparator(leftBoundary, null,
                NumberRangeComparator.InclusionType.Excluded, NumberRangeComparator.InclusionType.Excluded);

        Assertions.assertFalse(numberRangeComparator.compare(new SqlNumberValue(1)));
        Assertions.assertTrue(numberRangeComparator.compare(new SqlNumberValue(4)));
        Assertions.assertTrue(numberRangeComparator.compare(new SqlNumberValue(5)));
        Assertions.assertFalse(numberRangeComparator.compare(new SqlNumberValue(0)));
        Assertions.assertTrue(numberRangeComparator.compare(new SqlNumberValue(100)));
    }

    @Test
    public void test_compareValueWithinRange_4() {
        NumberRangeComparator numberRangeComparator = new NumberRangeComparator(null, null,
                NumberRangeComparator.InclusionType.Excluded, NumberRangeComparator.InclusionType.Excluded);

        Assertions.assertTrue(numberRangeComparator.compare(new SqlNumberValue(1)));
        Assertions.assertTrue(numberRangeComparator.compare(new SqlNumberValue(4)));
        Assertions.assertTrue(numberRangeComparator.compare(new SqlNumberValue(5)));
        Assertions.assertTrue(numberRangeComparator.compare(new SqlNumberValue(0)));
        Assertions.assertTrue(numberRangeComparator.compare(new SqlNumberValue(100)));
    }

    @Test
    public void test_intersectRange_1() {
        SqlNumberValue leftBoundary1 = new SqlNumberValue(1);
        SqlNumberValue rightBoundary1 = new SqlNumberValue(4);

        NumberRangeComparator numberRangeComparator1 = new NumberRangeComparator(leftBoundary1, rightBoundary1,
                NumberRangeComparator.InclusionType.Included, NumberRangeComparator.InclusionType.Included);

        SqlNumberValue leftBoundary2 = new SqlNumberValue(4);
        SqlNumberValue rightBoundary2 = new SqlNumberValue(10);

        NumberRangeComparator numberRangeComparator2 = new NumberRangeComparator(leftBoundary2, rightBoundary2,
                NumberRangeComparator.InclusionType.Excluded, NumberRangeComparator.InclusionType.Included);

        Optional<ValueComparator<Integer>> intersection = numberRangeComparator1.intersect(numberRangeComparator2);

        Assertions.assertFalse(intersection.isPresent());
    }

    @Test
    public void test_intersectRange_2() {
        SqlNumberValue leftBoundary1 = new SqlNumberValue(1);
        SqlNumberValue rightBoundary1 = new SqlNumberValue(4);

        NumberRangeComparator numberRangeComparator1 = new NumberRangeComparator(leftBoundary1, rightBoundary1,
                NumberRangeComparator.InclusionType.Included, NumberRangeComparator.InclusionType.Included);

        SqlNumberValue leftBoundary2 = new SqlNumberValue(4);
        SqlNumberValue rightBoundary2 = new SqlNumberValue(10);

        NumberRangeComparator numberRangeComparator2 = new NumberRangeComparator(leftBoundary2, rightBoundary2,
                NumberRangeComparator.InclusionType.Included, NumberRangeComparator.InclusionType.Included);

        Optional<ValueComparator<Integer>> intersection = numberRangeComparator1.intersect(numberRangeComparator2);

        Assertions.assertTrue(intersection.isPresent());
        Assertions.assertTrue(intersection.get() instanceof NumberEqualityComparator);

        intersection.ifPresent(valueComparator -> {
            Assertions.assertFalse(valueComparator.compare(new SqlNumberValue(1)));
            Assertions.assertTrue(valueComparator.compare(new SqlNumberValue(4)));
            Assertions.assertFalse(valueComparator.compare(new SqlNumberValue(10)));
        });
    }

    @Test
    public void test_intersectRange_3() {
        NumberRangeComparator numberRangeComparator1 = new NumberRangeComparator(null, null,
                NumberRangeComparator.InclusionType.Included, NumberRangeComparator.InclusionType.Included);

        SqlNumberValue leftBoundary2 = new SqlNumberValue(4);
        SqlNumberValue rightBoundary2 = new SqlNumberValue(10);

        NumberRangeComparator numberRangeComparator2 = new NumberRangeComparator(leftBoundary2, rightBoundary2,
                NumberRangeComparator.InclusionType.Included, NumberRangeComparator.InclusionType.Included);

        Optional<ValueComparator<Integer>> intersection = numberRangeComparator1.intersect(numberRangeComparator2);

        Assertions.assertTrue(intersection.isPresent());

        intersection.ifPresent(valueComparator -> {
            Assertions.assertFalse(valueComparator.compare(new SqlNumberValue(1)));
            Assertions.assertTrue(valueComparator.compare(new SqlNumberValue(4)));
            Assertions.assertTrue(valueComparator.compare(new SqlNumberValue(10)));
            Assertions.assertFalse(valueComparator.compare(new SqlNumberValue(11)));
        });
    }

    @Test
    public void test_intersectRange_4() {
        NumberEqualityComparator numberEqualityComparator = new NumberEqualityComparator(new SqlNumberValue(5), true);

        SqlNumberValue leftBoundary2 = new SqlNumberValue(4);
        SqlNumberValue rightBoundary2 = new SqlNumberValue(10);

        NumberRangeComparator numberRangeComparator = new NumberRangeComparator(leftBoundary2, rightBoundary2,
                NumberRangeComparator.InclusionType.Included, NumberRangeComparator.InclusionType.Included);

        Optional<ValueComparator<Integer>> intersection = numberRangeComparator.intersect(numberEqualityComparator);

        Assertions.assertTrue(intersection.isPresent());
        Assertions.assertTrue(intersection.get() instanceof NumberEqualityComparator);

        intersection.ifPresent(valueComparator -> {
            Assertions.assertFalse(valueComparator.compare(new SqlNumberValue(1)));
            Assertions.assertTrue(valueComparator.compare(new SqlNumberValue(5)));
            Assertions.assertFalse(valueComparator.compare(new SqlNumberValue(10)));
            Assertions.assertFalse(valueComparator.compare(new SqlNumberValue(11)));
        });
    }

    @Test
    public void test_intersectRange_5() {
        NumberEqualityComparator numberEqualityComparator = new NumberEqualityComparator(new SqlNumberValue(5), false);

        SqlNumberValue leftBoundary2 = new SqlNumberValue(4);
        SqlNumberValue rightBoundary2 = new SqlNumberValue(10);

        NumberRangeComparator numberRangeComparator = new NumberRangeComparator(leftBoundary2, rightBoundary2,
                NumberRangeComparator.InclusionType.Included, NumberRangeComparator.InclusionType.Included);

        Optional<ValueComparator<Integer>> intersection = numberRangeComparator.intersect(numberEqualityComparator);

        Assertions.assertTrue(intersection.isPresent());

        intersection.ifPresent(valueComparator -> {
            Assertions.assertFalse(valueComparator.compare(new SqlNumberValue(1)));
            Assertions.assertFalse(valueComparator.compare(new SqlNumberValue(5)));
            Assertions.assertTrue(valueComparator.compare(new SqlNumberValue(10)));
            Assertions.assertFalse(valueComparator.compare(new SqlNumberValue(11)));
        });
    }
}