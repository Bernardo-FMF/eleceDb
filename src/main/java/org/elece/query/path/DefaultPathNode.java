package org.elece.query.path;

import org.elece.query.comparator.EqualityComparator;
import org.elece.query.comparator.ValueComparator;

import java.util.Objects;

public class DefaultPathNode {
    private final String columnName;
    private final ValueComparator<?> valueComparator;
    private final IndexType indexType;

    public DefaultPathNode(String columnName,
                           ValueComparator<?> valueComparator,
                           IndexType indexType) {
        this.columnName = columnName;
        this.valueComparator = valueComparator;
        this.indexType = indexType;
    }

    public ValueComparator<?> getValueComparator() {
        return valueComparator;
    }

    public String getColumnName() {
        return columnName;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public int getPriority() {
        int indexTypePriority = indexType.getPriority() == 0 ? 0x00 : 0xF0;
        int comparatorPriority = valueComparator instanceof EqualityComparator<?> ? 0x0F : 0x00;
        return indexTypePriority | comparatorPriority;
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
        return Objects.equals(columnName, that.columnName) && Objects.equals(valueComparator, that.valueComparator) && indexType == that.indexType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, valueComparator, indexType);
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
