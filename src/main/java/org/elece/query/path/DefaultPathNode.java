package org.elece.query.path;

import org.elece.query.comparator.EqualityComparator;
import org.elece.query.comparator.ValueComparator;

import java.util.Objects;

public class DefaultPathNode {
    private final String column;
    private final ValueComparator<?> valueComparator;
    private final IndexType indexType;

    public DefaultPathNode(String column, ValueComparator<?> valueComparator, IndexType indexType) {
        this.column = column;
        this.valueComparator = valueComparator;
        this.indexType = indexType;
    }

    public ValueComparator<?> getValueComparator() {
        return valueComparator;
    }

    public String getColumn() {
        return column;
    }

    public int getPriority() {
        // TODO
        int priority = 0x00;
        priority = priority | (0x10 << indexType.getPriority());
        if (valueComparator instanceof EqualityComparator<?>) {
            priority = priority | 0x01;
        }
        return priority;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DefaultPathNode that = (DefaultPathNode) obj;
        return Objects.equals(column, that.column) && Objects.equals(valueComparator, that.valueComparator) && indexType == that.indexType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, valueComparator, indexType);
    }

    public enum IndexType {
        NonIndexed(0),
        Indexed(1);

        private final int priority;

        IndexType(int priority) {
            this.priority = priority;
        }

        public static IndexType fromBoolean(boolean isIndexed) {
            return isIndexed ? Indexed : NonIndexed;
        }

        public int getPriority() {
            return priority;
        }
    }
}
