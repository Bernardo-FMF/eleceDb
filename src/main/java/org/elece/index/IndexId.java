package org.elece.index;

public class IndexId {
    private final int tableId;
    private final int columnId;

    public IndexId(int tableId, int columnId) {
        this.tableId = tableId;
        this.columnId = columnId;
    }

    public String asString() {
        return String.format("%d_%d", tableId, columnId);
    }

    public int asInt() {
        return asString().hashCode();
    }
}
